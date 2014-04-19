var url = require( "url" );
var assert = require( "assert" );
var crypto = require( "crypto" );
var querystring = require( "querystring" );

var backbone = require( "backbone" );
var mongodb = require( "mongodb" );
var uuid = require( "node-uuid" );
var _ = require( "underscore" );
var async = require( "async" );
var sift = require( "sift" );

var backsync = require( ".." );

var md5 = function( str ) {
    return crypto.createHash( "md5" ).update( str ).digest( "hex" );
}

var modelsToObject = function( models ) {
    return models.reduce(function( memo, model ) {
        var obj = {}
        obj[ model.id ] = model.toJSON();
        _.extend( memo, obj )
        return memo
    }, {});
};

var test_collection = function( Collection, done ) {
    var Model = Collection.prototype.model;
    var data = [
        { age: 30, color: "blue", id: "1" },
        { age: 2, color: "green", id: "2" },
        { age: 15, color: "blue", id: "3" },
        { age: 49, color: "blue", id: "4" },
        { age: 7, color: "blue", id: "5" }
    ];

    var save_multi = data.map( function( datum ) {
        return function( cb ) {
            new Model().save( datum, {
                success: function( m ) { cb( null, m ) }
            });
        }
    })

    async.series([
        function( cb ) { // save the documents
            async.series( save_multi, cb )
        },

        function( cb ) { // search with filters, sort, skip and limit
            new Collection().fetch({
                data: {
                    color: "blue",
                    $sort: "age",
                    $skip: 1,
                    $limit: 2
                }, success: function( c ) {
                    var ids = c.map( function( m ) { return m.id } );
                    var ages = c.map( function( m ) { return m.get( "age" ) });
                    assert( _.every( ids ) ); // all docs have an id
                    assert.deepEqual( ages, [ 15, 30 ] );
                    cb()
                }
            })
        },

        function( cb ) { // only filter, maintains native order
            new Collection().fetch({
                data: {
                    color: "blue",
                }, success: function( c ) {
                    var ages = c.map( function( m ) { return m.get( "age" ) });
                    assert.deepEqual( ages, [ 30, 15, 49, 7 ] );
                    cb()
                }
            })
        }
    ], done );
};

var test_model = function( Model, done ) {
    async.waterfall([
        function( cb ) { // create
            new Model().save( { hello: "world" }, {
                success: function( m ) { cb( null, m ) },
                error: function( m, err ) { cb( err ) }
            });
        },
        function( m, cb ) { // update
            new Model({ id: m.id }).save( { foo: "bar" }, {
                success: function( m ) { cb( null, m ) },
                error: function( m, err ) { cb( err ) }
            });
        },
        function( m, cb ) { // patch
            new Model({ id: m.id }).save( { alice: "bob" }, {
                patch: true,
                success: function( m ) { cb( null, m ) },
                error: function( m, err ) { cb( err ) }
            });
        },
        function( _m, cb ) { // read
            new Model({ id: _m.id }).fetch({
                success: function( m ) {
                    assert( m.id );
                    assert.equal( m.id, _m.id );
                    assert( !m.get( "hello" ) ); // removed by update
                    assert.equal( typeof m._id, "undefined" );
                    assert.equal( m.get( "foo" ), "bar" );
                    assert.equal( m.get( "alice" ), "bob" );
                    cb( null, m )
                },
                error: function( m, err ) { cb( err ) }
            });
        },
        function( m, cb ) { // delete
            new Model({ id: m.id }).destroy({
                success: function( m ) { cb( null, m ); },
                error: function( m, err ) { cb( err ) }
            });
        },
        function( m, cb ) { // confirm it's deleted
            new Model({ id: m.id }).fetch({
                error: function( m, err ) {
                    assert.equal( err.name, "NotFoundError" );
                    cb();
                }
            });
        }
    ], done )
}

describe( "backsync", function() {

    it( "fails if the backsync method isn't implemented", function() {
        var Model = backbone.Model.extend({
            urlRoot: "/fairy",
            sync: backsync()
        });

        assert.throws( function() {
            new Model().save();
        });
    });


    it( "fails if no url is specified", function() {
        var Model = backbone.Model.extend({
            url: null,
            sync: backsync()
        });

        assert.throws( function() {
            new Model().save();
        });
    });


    it( "fails to read a model without an id", function( done ) {
        var Model = backbone.Model.extend({
            urlRoot: "/test",
            sync: backsync()
        });

        new Model({ id: null }).once( "error", function( m, err ) {
            assert( err.message.match( /missing or incorrect model id/i ) );
            done();
        }).once( "sync", function() {
            assert.fail( "loaded", "Missing or Incorrect Model ID" );
        }).fetch();
    });


});

describe( "backsync.memory", function() {

    var datastore = {};
    var Model = backbone.Model.extend({
        urlRoot: "/somethings",
        sync: backsync.memory( datastore )
    });

    var Collection = backbone.Collection.extend({
        model: Model,
        url: Model.prototype.urlRoot,
        sync: backsync.memory( datastore )
    });

    beforeEach(function() {
        for ( var i in datastore ) {
            delete datastore[ i ];
        }
    });

    it( "implements the middleware CRUD API", function( done ) {
        test_model( Model, done )
    });

    it( "implements the collection search", function( done ) {
        test_collection( Collection, done );
    });

});


describe( "backsync.couchdb", function() {

    var data = {};
    var mock_request = function( opts, cb ) {
        var _url = url.parse( opts.url );
        var qs = querystring.parse( _url.query )
        var db = _url.pathname.split( "/" )[ 1 ];
        var id = _url.pathname.split( "/" )[ 2 ];

        data[ _url.host ] || ( data[ _url.host ] = {} );
        data[ _url.host ][ db ] || ( data[ _url.host ][ db ] = {} );
        var d = data[ _url.host ][ db ];
        var res = null;
        if ( opts.method == "PUT" ) {
            if ( d[ id ] && d[ id ].rev != qs.rev ) {
                res = { "error": "conflict" }
            } else {
                d[ id ] = {
                    doc: JSON.parse( opts.body ),
                    rev: md5( uuid.v4() ),
                    id: id.split( "/" ).pop()
                };
                res = { ok: "true", id: d[ id ].id, rev: d[ id ].rev };
            }
        } else if ( opts.method == "GET" || !opts.method ) {
            if ( id == "_all_docs" ) {
                if ( qs.limit ) qs.limit = +qs.limit;
                if ( qs.startkey ) qs.startkey = JSON.parse( qs.startkey );
                if ( qs.endkey ) qs.endkey = JSON.parse( qs.endkey );

                d = _.sortBy( _.values( d ), "id" );
                res = {
                    total_rows: d.length,
                    offset: 0,
                    rows: [],
                };
                for ( var i = 0 ; i < d.length ; i += 1 ) {
                    var doc = d[ i ];
                    if ( qs.startkey && doc.id < qs.startkey ) {
                        res.offset += 1;
                    } else if ( qs.endkey && doc.id > qs.endkey ) {
                        break;
                    } else if ( qs.limit && res.rows.length >= qs.limit ) {
                        break;
                    } else {
                        var row = _.clone( doc )
                        row.doc._id = doc.id
                        row.doc._rev = doc.rev;
                        row.value = { rev: doc.rev };
                        delete row.rev;
                        if ( !qs.include_docs ) { delete row.doc }
                        res.rows.push( row );
                    }
                }
            } else if ( !d[ id ] ) {
                res = { error: "not_found" };
            } else {
                res = _.clone( d[ id ].doc );
                res._rev = d[ id ].rev;
                res._id = d[ id ].id;
            }
        } else if ( opts.method == "DELETE" ) {
            if ( !d[ id ] ) {
                res = { error: "not_found" };
            } else if ( d[ id ].rev != qs.rev ) {
                res = { error: "conflict" };
            } else {
                delete d[ id ];
                res = { ok: "true" };
            }
        }

        var r = {};
        process.nextTick(function() {
            cb.call( r, null, {}, JSON.stringify( res ) )
        });
        return r;
    }

    var Model = backbone.Model.extend({
        urlRoot: "http://127.0.0.1:5984/test_backsyncx",
        sync: backsync.couchdb({ create_db: true, request: mock_request })
    });

    var Collection = backbone.Collection.extend({
        model: Model,
        url: Model.prototype.urlRoot,
        sync: Model.prototype.sync
    });


    it( "implements the model CRUD API", function( done ) {
        test_model( Model, done )
    });


    it( "implements the collection search", function( done ) {
        test_collection( Collection, done );
    });

    it( "doesn't save the id and rev on the object", function( done ) {
        async.waterfall([
            function( cb ) {
                new Model({ id: "cookie", hello: "world" }).save( null, {
                    error: function( m, err ) { cb( err ) },
                    success: function( m ) { cb( null, m ) }
                })
            },

            function( m, cb ) {
                assert.equal( m.id, "cookie" );
                assert( m.get( "rev" ) ); // rev was defined
                new Model({ id: "cookie", rev: m.get( "rev" ) }).save( null, {
                    error: function( m, err ) { cb( err ) },
                    success: function( m ) {
                        var db = data[ "127.0.0.1:5984" ][ "test_backsyncx" ];
                        var r = db[ "cookie" ].doc;
                        assert( !r[ "hello" ] ); // overwrite
                        assert( !r[ "id" ] );
                        assert( !r[ "rev" ] );
                        cb();
                    }
                })
            }
        ], done );
    });


    it( "transform id-filters to start- and end-key", function( done ) {
        var C = backbone.Collection.extend({
            model: Model,
            url: "http://127.0.0.1:5984/test_backsyncx",
            sync: backsync.couchdb({
                request: function( opts, cb ) {
                    var qs = querystring.parse( url.parse( opts.url ).query );
                    assert.equal( qs.startkey, '"ab"' )
                    assert.equal( qs.endkey, '"ac"' )
                    assert.equal( qs.inclusive_end, "false" );
                    done();
                    return {};
                }
            })
        });

        new C().fetch({
            data: { id: { $gt: "ab", $lt: "ac" } }
        });
    });


    it( "implements the sort, limit, skip and filters", function( done ) {
        var host = "127.0.0.1:5984";
        var db = "test_backsyncx_info"
        var C = backbone.Collection.extend({
            model: Model,
            url: "http://" + host + "/" + db,
            sync: backsync.couchdb({ request: mock_request })
        });

        data[ host ] || ( data[ host ] = {} );
        data[ host ][ db ] = {
            "a1": { doc: { _id: "a1", color: "red" }, id: "a1", rev: 5 },

            "a2": { doc: { _id: "a2", color: "blue" }, id: "a2", rev: 5 },
            "a3": { doc: { _id: "a3", color: "red" }, id: "a3", rev: 3 },
            "a4": { doc: { _id: "a4", color: "blue" }, id: "a4", rev: 2 },
            "b5": { doc: { _id: "b5", color: "blue" }, id: "b5", rev: 2 },
            "b6": { doc: { _id: "b6", color: "blue" }, id: "b6", rev: 1 },

            "b7": { doc: { _id: "b7", color: "red" }, id: "b7", rev: 1 },
            "b8": { doc: { _id: "b8", color: "blue" }, id: "b8", rev: 5 },
        };

        var req;
        var c = new C();
        c.on( "request", function( c, _req ) { req = _req });
        c.sync( "read", c, {
            data: {
                id: { $gte: "a2", $lte: "b7" },
                color: "blue",
                $limit: 3,
                $sort: "rev",
                $skip: 1
            },
            backsync: { couchdb: { request_limit: 3 } },
            success: function( res ) {
                assert.equal( req.body.total_rows, 8 );
                assert.deepEqual( res, [
                    { color: "blue", id: "a4", rev: 2 },
                    { color: "blue", id: "b5", rev: 2 },
                    { color: "blue", id: "a2", rev: 5 },
                ]);
                done()
            },
        });
    });


    it( "uses the default dsn", function( done ) {
        var dsn = "http://127.0.0.2:5984/test_backsyncx";
        var M = backbone.Model.extend({
            urlRoot: "/models",
            sync: backsync.couchdb({ create_db: true, request: mock_request, dsn: dsn })
        });

        new M().save({ hello: "world" }, {
            success: function( m ) {
                var doc = data[ "127.0.0.2:5984" ][ "test_backsyncx_models" ][ m.id ];
                assert( doc.doc[ "hello" ], "world" );
                done();
            }
        });

    });

});


describe( "backsync.mongodb", function() {

    var dsn = "mongodb://127.0.0.1:27017/test_backsyncx";
    beforeEach(function() {
        mock_client._dbs = {};
    });

    var mock_collection = function() {
        _data = {};
        return {
            insert: function( doc, cb ) {
                if ( !doc._id ) { doc._id = md5( uuid.v4() ) }
                _data[ doc._id ] = _.clone( doc );
                var res = _.clone( _data[ doc._id ] );
                process.nextTick( function() { cb( null, [ res ] ) } );
            },
            save: function( doc, cb ) {
                this.insert.apply( this, arguments );
            },
            findAndModify: function( spec, sort, doc, opts, cb ) {
                _.extend( _data[ spec._id ], doc.$set );
                var res = _.clone( _data[ spec._id ] );
                process.nextTick( function() { cb( null, res ) } );
            },
            findOne: function( spec, cb ) {
                var res = _.clone( _data[ spec._id ] );
                process.nextTick( function() { cb( null, res ) } );
            },
            remove: function( spec, cb ) {
                var res = 0;
                if ( _data[ spec._id ] ) {
                    res = 1;
                    delete _data[ spec._id ];
                }
                process.nextTick( function() { cb( null, res ) } );
            },
            find: function( spec ) {
                spec || ( spec = {} );
                var sort = null, limit = null, skip = null;
                return {
                    sort: function( s ) { sort = s },
                    limit: function( l ) { limit = l },
                    skip: function( s ) { skip = s },
                    toArray: function( cb ) {
                        var res = sift( spec, _.values( _data ) ).reverse();
                        if ( sort ) { res = _.sortBy( res, sort ) }
                        if ( skip ) { res.splice( 0, skip ) }
                        if ( limit ) { res.splice( limit ) }
                        res = res.map(function( m ) { return _.clone( m ) });
                        process.nextTick( function() { cb( null, res ) } );
                    }
                }
            }
        };
    }
    var mock_client = {
        _dbs: {},
        connect: function( dsn, cb ) {
            if ( !this._dbs[ dsn ] ) { this._dbs[ dsn ] = {} }
            var collections = this._dbs[ dsn ];
            cb( null, {
                collection: function( name ) {
                    if ( !collections[ name ] ) {
                        collections[ name ] = mock_collection()
                    }
                    return collections[ name ];
                }
            });
        }
    };


    var Model = backbone.Model.extend({
        urlRoot: "mongodb://127.0.0.1:27017/test_backsyncx/models",
        sync: backsync.mongodb({ mongo_client: mock_client })
    });

    var Collection = backbone.Collection.extend({
        model: Model,
        url: Model.prototype.urlRoot,
        sync: Model.prototype.sync
    });

    it( "implements the middleware CRUD API", function( done ) {
        test_model( Model, done )
    });

    it( "implements the collection search", function( done ) {
        test_collection( Collection, done );
    });

    it( "generates the correct dsn", function( done ) {
        var mock_client = {
            connect: function( dsn, cb ) {
                comps = dsn.split( "/" )

                assert.equal( comps[ 0 ], "mongodb:" ); // protocol
                assert.equal( comps[ 2 ], "127.0.0.1:27017" ); // host
                assert.equal( comps[ 3 ], "test_backsyncx" ) // db
                assert.equal( comps.length, 4 );

                cb( null, {
                    collection: function( name ) {
                        assert.equal( name, "one/two/three" )
                        return { insert: function() {} }
                    }
                })

                done()
            }
        };

        var M = new Model();
        M.sync = backsync.mongodb({ mongo_client: mock_client })
        M.save({ hello: "world" }, {
            url: "mongodb://127.0.0.1:27017/test_backsyncx/one/two/three"
        })
    });


    it( "converts id to ObjectID", function( done ) {
        var mock_client = {
            connect: function( dsn, cb ) {
                cb( null, {
                    collection: function( name ) {
                        return {
                            save: function( doc ) {
                                assert.equal( doc.hello, "world" );
                                assert.equal( typeof doc._id, "object" )
                                assert.equal( doc._id.toString(), "531e096521b5c6670c5e63a3" );
                                assert( !doc.id );
                                done();
                            }
                        }
                    }
                });
            }
        };

        var M = new Model();
        M.sync = backsync.mongodb({ mongo_client: mock_client });
        M.save({ hello: "world", id: "531e096521b5c6670c5e63a3" }, {})
    });


    it( "uses the default dsn", function( done ) {
        var M = backbone.Model.extend({
            urlRoot: "/models",
            sync: backsync.mongodb({ dsn: dsn, mongo_client: mock_client })
        });

        new M().save({ hello: "world" }, {
            success: function( m ) {
                assert( m.get( "hello" ), "world" );
                var collection = mock_client._dbs[ dsn ].models;
                collection.find().toArray(function( err, docs ) {
                    // console.log( docs )
                    assert.equal( docs.length, 1 );
                    assert.equal( docs[ 0 ]._id, m.id );
                    assert.equal( docs[ 0 ].hello, "world" );
                    done();
                });
            }
        });

    });


    it( "generates md5 of uuid", function( done ) {
        var mock_client = {
            connect: function( dsn, cb ) {
                cb( null, {
                    collection: function( name ) {
                        return {
                            insert: function( doc ) {
                                assert.equal( typeof doc._id, "string" );
                                assert.equal( doc._id.length, 32 );
                                assert( !doc.id );
                                done();
                            }
                        }
                    }
                });
            }
        }

        var m = new Model();
        m.sync = backsync.mongodb({ use_uuid: true, mongo_client: mock_client });
        m.save({ hello: "world" }, {})
    });

});