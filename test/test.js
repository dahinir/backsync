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
                    var ages = c.map( function( m ) { return m.get( "age" ) });
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
                success: function( m ) { cb( null, m ) }
            });
        },
        function( m, cb ) { // update
            new Model({ id: m.id }).save( { foo: "bar" }, {
                success: function( m ) { cb( null, m ) },
                error: function( m, err ) { console.log( err )}
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
                    assert( !m.get( "hello" ) ); // removed by update
                    assert.equal( typeof m._id, "undefined" );
                    assert.equal( m.get( "foo" ), "bar" );
                    assert.equal( m.get( "alice" ), "bob" );
                    cb( null, m )
                }
            });
        },
        function( m, cb ) { // delete
            new Model({ id: m.id }).destroy({
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
        _url = _url.host + _url.pathname;

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
            } else if ( d[ _url ].rev != qs.rev ) {
                res = { error: "conflict" };
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
                }
            })
        });

        new C().fetch({
            data: { id: { $gt: "ab", $lt: "ac" } }
        });
    });


    it( "returns additional information", function( done ) {
        var C = backbone.Collection.extend({
            model: Model,
            url: "http://127.0.0.1:5984/test_backsyncx",
            sync: backsync.couchdb({
                request: function( opts, cb ) {
                    var qs = querystring.parse( url.parse( opts.url ).query );
                    var body = {
                        total_rows: data.length,
                        offset: 0,
                        rows: []
                    };
                    for ( var i = 0 ; i < data.length ; i += 1 ) {
                        var d = data[ i ];
                        if ( d._id < qs.startkey ) {
                            body.offset += 1;
                        } else if ( d._id <= qs.endkey ) {
                            body.rows.push({  doc: d, id: d._id, value: { rev: 5 } } );
                        } else {
                            break
                        }
                    }
                    cb( null, null, JSON.stringify( body ) );
                }
            })
        });

        var data = [
            { _id: "1", color: "red" }, { _id: "2", color: "blue" },
            { _id: "3", color: "red" }, { _id: "4", color: "blue" },
            { _id: "5", color: "blue" }, { _id: "6", color: "blue" },
            { _id: "7", color: "red" }, { _id: "8", color: "blue" },
        ];

        var c = new C();
        c.sync( "read", c, {
            data: { id: { $gte: 2, $lte: 6 }, color: "blue", $limit: 3 },
            info: true,
            success: function( res ) {
                assert.equal( res.total, data.length );
                assert.equal( res.offset, 1 );
                assert.equal( res.count, 5 );
                assert.equal( res.last_id, "6" );
                assert.deepEqual( res.results, [
                    { color: "blue", id: "2", rev: 5 },
                    { color: "blue", id: "4", rev: 5 },
                    { color: "blue", id: "5", rev: 5 },
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
                var doc = d[ "127.0.0.2:5984/test_backsyncx_models/" + m.id ]
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