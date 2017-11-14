from flask import Flask
from flask import render_template
from flask import request
from flask import jsonify
from flask import abort

import couchdb
from uuid import uuid4

app = Flask( __name__ )

couch = couchdb.Server( 'http://192.168.99.101:3306/' )

if 'matches' not in couch:
    couch.create( 'matches' )
match_table = couch[ 'matches' ]

if 'categories' not in couch:
    db = couch.create( 'categories' )
    default_categories = [
        'Rock', 'Pop', 'Electronica', 'Rap', 'Rock alternativo', 'Hip hop',
        'Reggae', 'Reggaeton', 'Bachata', 'Clasica', 'Balada', 'Salsa', 'Punk', 'Jazz',
        'Rock sinfonico', 'Grunge', 'Cumbia', 'Dance', 'Ska', 'Tecno', 'Disco',
        'Blues', 'Opera', 'Tango', 'Vallenato', 'Ranchera', 'Samba', 'Mambo',
        'Bolero', 'Protesta'
    ]
    for cat in default_categories:
        doc_id = uuid4( ).hex
        db[ doc_id ] = {
            'category_id' : uuid4( ).hex,
            'category_name' : cat
        }

category_table = couch[ 'categories' ]

if 'user_categories' not in couch:
    couch.create( 'user_categories' )
user_categories_table = couch[ 'user_categories' ]

match_doc = {
    'user_id' : -1,
    'file_id' : -1,
    'category_id' : -1
}

matches_by_user = ''' function( doc ) {
    emit( doc.user_id, [ doc.file_id, doc.category_id ] );
}
'''

matches_by_user_file_id = ''' function( doc ) {
    emit( doc.user_id, [ doc._id, doc.file_id ] )
}
'''

default_categories_view = ''' function( doc ) {
    emit( doc.category_id, doc.category_name )
}
'''

user_categories_view = ''' function( doc ) {
    emit( doc.user_id, [ doc.category_id, doc.category_name, doc._id ] )
}
'''

@app.route( '/' )
def index( ):
    return 'Classifier ms is working B|'

################################################ MATCH ROUTES ##########################################################

@app.route( '/user/<int:user_id>/match', methods=[ 'POST', 'PUT' ] )
def new_match( user_id ):
    if not request.json or not 'file_id' in request.json or not 'category_id' in request.json:
        abort( 400 )

    category_found = False
    results = category_table.query( default_categories_view )
    for row in results:
        if row.key == request.json[ 'category_id' ]:
            category_found = True
            break

    if not category_found:
        results = user_categories_table.query( user_categories_view )
        for row in results[ user_id ]:
            if row.value[ 0 ] == request.json[ 'category_id' ]:
                category_found = True
                break

    if not category_found:
        return jsonify({
            'error' : 'Category with id %s does not exist.' % ( request.json[ 'category_id' ] )
        }), 400

    match = {
        'user_id' : user_id,
        'file_id' : int( request.json[ 'file_id' ] ),
        'category_id' : request.json[ 'category_id' ]
    }

    results = match_table.query( matches_by_user_file_id )

    if request.method == 'POST':

        for row in results[ user_id ]:
            if row.value[ 1 ] == int( request.json[ 'file_id' ] ):
                return jsonify({
                    'error' : 'File with id %s already has a category match.' % ( request.json[ 'file_id' ] )
                }), 400

        match_id = uuid4( ).hex
        match_table[ match_id ] = match

        return jsonify( match ), 201

    elif request.method == 'PUT':

        for row in results[ user_id ]:
            if row.value[ 1 ] == int( request.json[ 'file_id' ] ):
                doc = match_table[ row.value[ 0 ] ]
                doc[ 'category_id' ] = request.json[ 'category_id' ]
                match_table[ row.value[ 0 ] ] = doc
                return jsonify( match ), 200

        return jsonify({
            'error' : 'There is no match for file with id %s.' % ( request.json[ 'file_id' ] )
        }), 400

@app.route( '/user/<int:user_id>/match/<int:file_id>', methods=[ 'DELETE' ] )
def destroy_match( user_id, file_id ):
    results = match_table.query( matches_by_user_file_id )

    for row in results[ user_id ]:
        if row.value[ 1 ] == file_id:
            del match_table[ row.value[ 0 ] ]
            return jsonify({
                'message': 'Match deleted.'
            }), 200

    return jsonify({
        'error' : 'There is no match for file with id %i.' % ( file_id )
    }), 400

@app.route( '/user/<int:user_id>/matches' )
def get_user_matches( user_id ):
    results = match_table.query( matches_by_user )
    data = []
    for row in results[ user_id ]:
        data.append({
            'file_id' : row.value[ 0 ],
            'category_id' : row.value[ 1 ]
        })

    return jsonify({
        'data' : data
    }), 200

############################################## END MATCH ROUTES ########################################################

######################################### DEFAULT CATEGORIES ROUTES ####################################################

@app.route( '/categories' )
def default_categories( ):
    results = category_table.query( default_categories_view )
    data = []
    for row in results:
        data.append({
            'category_id' : row.key,
            'category_name' : row.value
        })
    return jsonify( data ), 200

####################################### END DEFAULT CATEGORIES ROUTES ##################################################

########################################### USER CATEGORIES ROUTES #####################################################

@app.route( '/user/<int:user_id>/categories', methods=[ 'GET', 'POST', 'PUT', 'DELETE' ] )
def user_categories( user_id ):
    results = user_categories_table.query( user_categories_view )
    results2 = category_table.query( default_categories_view )

    if request.method == 'GET':
        data = []
        for row in results[ user_id ]:
            data.append({
                'category_id' : row.value[ 0 ],
                'category_name' : row.value[ 1 ]
            })
        return jsonify( data ), 200
    elif request.method == 'POST':
        if not request.json or not 'category_name' in request.json:
            abort( 400 )

        for row in results[ user_id ]:
            if row.value[ 1 ].lower( ) == request.json[ 'category_name' ].lower( ):
                return jsonify({
                    'error' : 'There is a category with the same name.'
                }), 400

        for row in results2:
            if row.value.lower( ) == request.json[ 'category_name' ].lower( ):
                return jsonify({
                    'error' : 'There is a category with the same name.'
                }), 400

        doc_id = uuid4( ).hex
        user_categories_table[ doc_id ] = {
            'user_id' : user_id,
            'category_id' : uuid4( ).hex,
            'category_name' : request.json[ 'category_name' ]
        }
        return jsonify( user_categories_table[ doc_id ] ), 201
    elif request.method == 'PUT':
        if not request.json or not 'category_id' in request.json or not 'category_name' in request.json:
            abort( 400 )

        for row in results[ user_id ]:
            if row.value[ 0 ] == request.json[ 'category_id' ]:
                doc = user_categories_table[ row.value[ 2 ] ]
                doc[ 'category_name' ] = request.json[ 'category_name' ]
                user_categories_table[ row.value[ 2 ] ] = doc

                return jsonify( user_categories_table[ row.value[ 2 ] ] ), 200


    elif request.method == 'DELETE':
        if not request.json or not 'category_id' in request.json:
            abort( 400 )

        for row in results[ user_id ]:
            if row.value[ 0 ] == request.json[ 'category_id' ]:
                del user_categories_table[ row.value[ 2 ] ]
                return jsonify({
                    'message': 'User category deleted.'
                }), 200

######################################### END USER CATEGORIES ROUTES ###################################################

############################################## QUERYING ROUTES #########################################################

@app.route( '/user/<int:user_id>/category_for_file/<int:file_id>' )
def category_for_file( user_id, file_id ):

    results = match_table.query( matches_by_user )
    for row in results[ user_id ]:
        if row.value[ 0 ] == file_id:

            cat_name = ""
            user_categories_results = user_categories_table.query( user_categories_view )
            default_categories_results = category_table.query( default_categories_view )

            found = False
            for k in default_categories_results[ row.value[ 1 ] ]:
                cat_name = k.value
                found = True
                break

            if not found:
                for k in user_categories_results[ user_id ]:
                    if k.value[ 0 ] == row.value[ 1 ]:
                        cat_name = k.value[ 1 ]
                        break

            return jsonify({
                'category_id' : row.value[ 1 ],
                'category_name' : cat_name
            }), 200

    return jsonify({
        'error' : 'There is no file with id %i for user with id %i.' % ( file_id, user_id )
    })

@app.route( '/user/<int:user_id>/files_for_category/<string:category_id>' )
def files_for_category( user_id, category_id ):

    results = match_table.query( matches_by_user )
    data = []
    for row in results[ user_id ]:
        if category_id == row.value[ 1 ]:
            data.append({
                'file_id' : row.value[ 0 ]
            })
    return jsonify({
        'data' : data
    }), 200


############################################ END QUERYING ROUTES #######################################################

if __name__ == '__main__':
    app.run( host='0.0.0.0', port=3004 )
