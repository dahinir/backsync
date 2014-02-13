var mongodb = require( "mongodb" );

var backsync = require( ".." );

module.exports = function() {
    var connections = {};
    var connect = function( url, cb ) {
        if ( connections[ url ] ) {
            return cb( null, connections[ url ] );
        }
        mongodb.MongoClient.connect( url, function( err, db ) {
            if ( err ) return cb( err );
            connections[ url ] = db;
            cb( null, db );
        });
    };

    return backsync()
        .create( function( model, opts, cb ) {
            connect( opts.url_base, function( err, collection ) {
                if ( err ) return cb( err );
                collection.insert( model.toJSON(), function( err, doc ) {
                    if ( err ) return cb( err );
                    cb( null, doc );
                } );
            });

        }).update( function( model, opts, cb ) {
            connect( opts.url_base, function( err, collection ) {
                if ( err ) return cb( err );
                collection.save( model.toJSON(), function( err, doc ) {
                    if ( err ) return cb( err );
                    cb( null, doc );
                } );
            });

        }).patch( function( model, opts, cb ) {
            connect( opts.url_base, function( err, collection ) {
                if ( err ) return cb( err );

                var toupdate = { $set: model.toJSON() };
                collection.update({ _id: model.id }, toupdate, function( err, doc ) {
                    if ( err ) return cb( err );
                    cb( null, doc );
                } );
            });

        }).read( function( model, opts, cb ) {
            connect( opts.url_base, function( err, collection ) {
                if ( err ) return cb( err );

                collection.findOne({ _id: model.id }, function( err, doc ) {
                    if ( err ) return cb( err );
                    cb( null, doc );
                } );
            });

        }).remove( function( model, opts, cb ) {
            connect( opts.url_base, function( err, collection ) {
                if ( err ) return cb( err );

                collection.remove({ _id: model.id }, function( err, affected ) {
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

            var cursor = collection.find( data );
            if ( sort ) cursor.sort( sort );
            if ( skip ) cursor.skip( skip );
            if ( limit ) cursor.limit( limit );

            cursor.toArray(function( err, docs ) {
                if ( err ) return cb( err );
                cb( null, docs );
            });
        });
};