var url = require( "url" );
var assert = require( "assert" );
var crypto = require( "crypto" );
var querystring = require( "querystring" );

var backbone = require( "backbone" );
var mongodb = require( "mongodb" );
var uuid = require( "node-uuid" );
var _ = require( "underscore" );
var async = require( "async" );

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
        { age: 30, color: "blue" },
        { age: 2, color: "green" },
        { age: 15, color: "blue" },
        { age: 49, color: "blue" },
        { age: 7, color: "blue" }
    ];

    var save_multi = data.map( function( datum ) {
        return function( cb ) {
            new Model().save( datum, {
                success: function( m ) { cb( null, m ) }
            });
        }
    })

    async.series( save_multi, function( err, models ) {
        new Collection().fetch({
            data: {
                color: "blue",
                $sort: "age",
                $skip: 1,
                $limit: 2
            }, success: function( c ) {
                var ages = c.map( function( m ) { return m.get( "age" ) });
                assert.deepEqual( ages, [ 15, 30 ] );
                done();
            }
        })
    });
};

var test_model = function( Model, done ) {
    async.waterfall([
        function( cb ) { // create
            new Model().save( { hello: "world" }, {
                success: function( m ) { cb( null, m ) }
            });
        },
        function( m, cb ) { // update
            m.save( { foo: "bar" }, {
                success: function( m ) { cb( null, m ) }
            });
        },
        function( m, cb ) { // patch
            new Model({ id: m.id }).save( { alice: "bob" }, {
                patch: true,
                success: function( m ) { cb( null, m ) }
            });
        },
        function( m, cb ) { // read
            new Model({ id: m.id }).fetch({
                success: function( m ) {
                    assert( m.id );
                    assert.equal( typeof m._id, "undefined" );
                    assert.equal( m.get( "hello" ), "world" );
                    assert.equal( m.get( "foo" ), "bar" );
                    assert.equal( m.get( "alice" ), "bob" );
                    cb( null, m )
                }
            });
        },
        function( m, cb ) { // delete
            m.destroy({
                success: function( m ) { cb( null, m ); }
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

    var d = {};
    var mock_request = function( opts, cb ) {
        var _url = url.parse( opts.url );
        var qs = querystring.parse( _url.query )
        _url = _url.pathname;

        var res = null;
        if ( opts.method == "PUT" ) {
            if ( d[ _url ] && d[ _url ].rev != qs.rev ) {
                res = { "error": "conflict" }
            } else {
                d[ _url ] = {
                    doc: JSON.parse( opts.body ),
                    rev: md5( uuid.v4() ),
                    id: _url.split( "/" ).pop()
                };
                res = { ok: "true", id: d[ _url ].id, rev: d[ _url ].rev };
            }
        } else if ( opts.method == "GET" || !opts.method ) {
            if ( _url.indexOf( "_all_docs" ) != -1 ) {
                res = {
                    rows: _.map( d, function( v ) {
                        v = _.clone( v );
                        v.value = { rev: v.rev };
                        delete v.rev;
                        if ( !qs.include_docs ) { delete v.doc }
                        return v
                    })
                }
            } else if ( !d[ _url ] ) {
                res = { error: "not_found" };
            } else {
                res = _.clone( d[ _url ].doc );
                res._rev = d[ _url ].rev;
                res._id = d[ _url ].id;
            }
        } else if ( opts.method == "DELETE" ) {
            if ( !d[ _url ] ) {
                res = { error: "not_found" };
            } else {
                delete d[ _url ];
                res = { ok: "true" };
            }
        }

        process.nextTick(function() { cb( null, {}, JSON.stringify( res ) ) });
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

});


describe( "backsync.mongodb", function() {

    var collection = null;
    var dsn = "mongodb://127.0.0.1:27017/test_backsyncx";
    before(function( done ) {
        mongodb.MongoClient.connect( dsn, function( err, db ) {
            collection = db.collection( "models" );
            done();
        } )
    })

    beforeEach(function( done ) {
        collection.drop( function( err ) {
            if ( err && err.toString() != "MongoError: ns not found" ) throw err;
            done();
        });
    });

    var Model = backbone.Model.extend({
        urlRoot: "mongodb://127.0.0.1:27017/test_backsyncx/models",
        sync: backsync.mongodb()
    });

    var Collection = backbone.Collection.extend({
        model: Model,
        url: Model.prototype.urlRoot,
        sync: backsync.mongodb()
    });

    it( "implements the middleware CRUD API", function( done ) {
        test_model( Model, done )
    });

    it( "implements the collection search", function( done ) {
        test_collection( Collection, done );
    });

    it( "generates the correct dsn", function( done ) {
        connect = mongodb.MongoClient.connect
        mongodb.MongoClient.connect = function( dsn, cb ) {
            mongodb.MongoClient.connect = connect
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

        new Model().save({ hello: "world" }, {
            url: "mongodb://127.0.0.1:27017/test_backsyncx/one/two/three",
            // success: function() {}
        })
    });


    it( "converts id to ObjectID", function( done ) {
        connect = mongodb.MongoClient.connect
        mongodb.MongoClient.connect = function( dsn, cb ) {
            mongodb.MongoClient.connect = connect
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

        new Model().save({ hello: "world", id: "531e096521b5c6670c5e63a3" }, {})
    });


    it( "uses the default dsn", function( done ) {
        var M = backbone.Model.extend({
            urlRoot: "/models",
            sync: backsync.mongodb({ dsn: dsn })
        });

        new M().save({ hello: "world" }, {
            success: function( m ) {
                assert( m.get( "hello" ), "world" );
                collection.find({}).toArray(function( err, docs ) {
                    assert.equal( docs.length, 1 );
                    assert.equal( docs[ 0 ]._id, m.id );
                    assert.equal( docs[ 0 ].hello, "world" );
                    done();
                });
            }
        });

    });


    it( "generates md5 of uuid", function( done ) {
        connect = mongodb.MongoClient.connect
        mongodb.MongoClient.connect = function( dsn, cb ) {
            mongodb.MongoClient.connect = connect
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

        var m = new Model();
        m.sync = backsync.mongodb({ use_uuid: true });
        m.save({ hello: "world" }, {})
    });

});