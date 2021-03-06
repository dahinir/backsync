var _ = require( "underscore" );
var uuid = require( "node-uuid" );
var sift = require( "sift" );

var backsync = require( ".." );

var globalstore = {};
module.exports = function( datastore ) {
    var ds = datastore || globalstore;
    return backsync()
        .create( function( model, opts, cb ) {
            var id = uuid.v4();
            ds[ opts.url_base ] || ( ds[ opts.url_base ] = {} );
            ds[ opts.url_base ][ id ] = _.extend( {}, model.toJSON(), { id: id } );

            process.nextTick(function() {
                cb( null, _( ds[ opts.url_base ][ id ] ).clone() );
            });
            model.trigger( "request", model, { ds: ds, id: id }, opts );

        }).update( function( model, opts, cb ) {
            var id = opts.url_id;
            ds[ opts.url_base ] || ( ds[ opts.url_base ] = {} );
            ds[ opts.url_base ][ id ] = _.extend( {}, model.toJSON(), { id: id } );

            process.nextTick(function() {
                cb( null, _( ds[ opts.url_base ][ id ] ).clone() );
            });
            model.trigger( "request", model, { ds: ds, id: id }, opts );

        }).patch( function( model, opts, cb ) {
            var id = opts.url_id;
            ds[ opts.url_base ] || ( ds[ opts.url_base ] = {} );
            var org = ds[ opts.url_base ][ id ] || {};
            ds[ opts.url_base ][ id ] = _.extend( org, opts.attrs, { id: id } );

            process.nextTick(function() {
                cb( null, _( ds[ opts.url_base ][ id ] ).clone() );
            });
            model.trigger( "request", model, { ds: ds, id: id }, opts );

        }).read( function( model, opts, cb ) {
            var id = opts.url_id;

            process.nextTick(function() {
                if ( !ds[ opts.url_base ] || !ds[ opts.url_base ][ id ] ) {
                    return cb( new backsync.NotFoundError( opts.url_id ) )
                }
                cb( null, _( ds[ opts.url_base ][ id ] ).clone() );
            });
            model.trigger( "request", model, { ds: ds, id: id }, opts );

        }).delete( function( model, opts, cb ) {
            var id = opts.url_id, old = null, err = null
            if ( !ds[ opts.url_base ] || !ds[ opts.url_base ][ id ] ) {
               err = new backsync.NotFoundError( opts.url_id );
            } else {
                old = ds[ opts.url_base ][ id ];
                delete ds[ opts.url_base ][ id ];
            }

            process.nextTick(function() {
                cb( err, old );
            });
            model.trigger( "request", model, { ds: ds, id: id }, opts );

        }).search( function( collection, opts, cb ) {
            var models = _( ds[ opts.url_base ] ).values() || [],
                data = _.clone( opts.data || {} ),
                sort = data[ "$sort" ] || null,
                skip = data[ "$skip" ] || 0,
                limit = data[ "$limit" ] || models.length;

            delete data[ "$sort" ];
            delete data[ "$skip" ];
            delete data[ "$limit" ];
            models = sift( data, models ).reverse(); // sift reverses the order

            // sort
            if ( sort ) {
                models = _( models ).sortBy( sort );
            }

            models = models.slice( skip, skip + limit )

            process.nextTick(function() {
                cb( null, models )
            });
            collection.trigger( "request", collection, { ds: ds }, opts );
        });
};
