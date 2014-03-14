var mongodb = require( "mongodb" );

var _ = require( "underscore" );
var backsync = require( ".." );

var reObjectID = /^[0-9a-fA-F]{24}$/;

module.exports = function() {
    var connections = {};
    var connect = function( url, cb ) {
        var dsn = url.split( "/" );
        var collection = dsn.slice( 4 ).join( "/" ); // everything after the 4th slash
        dsn = dsn.slice( 0, 4 ).join( "/" );
        if ( connections[ dsn ] ) {
            return cb( null, connections[ url ].collection( collection ) );
        }
        mongodb.MongoClient.connect( dsn, function( err, db ) {
            if ( err ) return cb( err );
            connections[ url ] = db;
            cb( null, db.collection( collection ) );
        });
    };

    var serialize = function( model ) {
        var doc = model.toJSON();
        if ( doc.id ) {
            doc._id = doc.id;
            delete doc.id;
        }

        // convert ObjectID strings into objectIDs
        if ( doc._id && doc._id.match && doc._id.match( reObjectID ) ) {
            doc._id = mongodb.ObjectID( doc._id );
        }

        return doc;
    };

    var deserialize = function( doc ) {
        if ( doc._id ) {
            doc.id = doc._id.toString();
            delete doc._id;
        }
        return doc;
    }

    return backsync()
        .create( function( model, opts, cb ) {
            connect( opts.url_base, function( err, collection ) {
                if ( err ) return cb( err );

                collection.insert( serialize( model ), function( err, docs ) {
                    if ( !err && docs.length < 1 ) {
                        err = new Error( "No document was created" );
                    } else if ( !err && docs.length > 1 ) {
                        err = new Error( "Too many documents were created" );
                    }
                    if ( err ) return cb( err );

                    cb( null, deserialize( docs[ 0 ] ) );
                } );
            });

        }).update( function( model, opts, cb ) {
            connect( opts.url_base, function( err, collection ) {
                if ( err ) return cb( err );

                doc = serialize( model )
                collection.save( doc, function( err ) {
                    if ( err ) return cb( err );
                    collection.findOne({ _id: doc._id }, function( err, doc ) {
                        if ( err ) return cb( err );
                        cb( null, deserialize( doc ) );
                    });
                } );
            });

        }).patch( function( model, opts, cb ) {
            connect( opts.url_base, function( err, collection ) {
                if ( err ) return cb( err );

                var update = { $set: serialize( model ) };
                id = update.$set._id;
                delete update.$set._id; // don't set the id
                collection.update({ _id: id }, update, function( err ) {
                    if ( err ) return cb( err );
                    collection.findOne({ _id: id }, function( err, doc ) {
                        if ( err ) return cb( err );
                        cb( null, deserialize( doc ) );
                    });
                } );
            });

        }).read( function( model, opts, cb ) {
            connect( opts.url_base, function( err, collection ) {
                if ( err ) return cb( err );

                ;
                collection.findOne({ _id: serialize( model )._id }, function( err, doc ) {
                    if ( !err && !doc ) {
                        err = new backsync.NotFoundError( opts.url_id );
                    }
                    if ( err ) return cb( err );
                    cb( null, doc );
                } );
            });

        }).remove( function( model, opts, cb ) {
            connect( opts.url_base, function( err, collection ) {
                if ( err ) return cb( err );

                collection.remove({ _id: serialize( model )._id }, function( err, affected ) {
                    if ( !err && !affected ) {
                        err = new backsync.NotFoundError( opts.url_id );
                    }
                    if ( err ) return cb( err );
                    cb( null, model.toJSON() );
                } );
            });

        }).search( function( model, opts, cb ) {
            var data = opts.data || {},
                sort = data[ "$sort" ] || null,
                skip = data[ "$skip" ] || null,
                limit = data[ "$limit" ] || null;

            delete data[ "$sort" ];
            delete data[ "$skip" ];
            delete data[ "$limit" ];

            connect( opts.url_base, function( err, collection ) {
                if ( err ) return cb( err );
                var cursor = collection.find( data );
                if ( sort ) cursor.sort( sort );
                if ( skip ) cursor.skip( skip );
                if ( limit ) cursor.limit( limit );

                cursor.toArray(function( err, docs ) {
                    if ( err ) return cb( err );
                    cb( null, docs.map( deserialize ) );
                });
            });
        });
};