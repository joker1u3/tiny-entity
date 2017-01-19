"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments)).next());
    });
};
const Datastore = require("nedb");
const nedbPool_1 = require("./nedbPool");
var dbconfig;
class NeDBDataContext {
    constructor(config) {
        this.transList = [];
        this.config = config;
        dbconfig = config;
        if (!config.IsMulitTabel) {
            this.nedb = new Datastore(config.FilePath + config.DBName);
            this.nedb.loadDatabase();
        }
    }
    Create(obj) {
        return __awaiter(this, void 0, void 0, function* () {
            delete obj.ctx;
            try {
                let db = yield nedbPool_1.NeDBPool.Current.GetDBConnection(obj.toString(), this.config);
                let r = yield this.CreateInner(obj, db);
                this.PushQuery("create", { id: obj.id });
                return r;
            }
            catch (err) {
                if (err.errorType == "uniqueViolated") {
                    throw { code: -101, message: "插入失败：重复的主键id" };
                }
                else {
                    throw err;
                }
            }
        });
    }
    CreateInner(obj, db) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                db.insert(obj, (err, r) => {
                    if (err)
                        reject(err);
                    else {
                        resolve(r);
                    }
                });
            });
        });
    }
    UpdateRange(list) {
        return __awaiter(this, void 0, void 0, function* () {
            let entityList = [];
            for (let item of list) {
                let v = yield this.Update(item);
                entityList.push(v);
            }
            return entityList;
        });
    }
    Update(obj) {
        return __awaiter(this, void 0, void 0, function* () {
            delete obj.ctx;
            let db = yield nedbPool_1.NeDBPool.Current.GetDBConnection(obj.toString(), this.config);
            let entity;
            if (this.transOn) {
                entity = yield this.GetEntity(obj.toString(), obj.id, db);
                entity.toString = obj.toString;
            }
            let r = yield this.UpdateInner(obj, db);
            this.PushQuery("update", entity);
            return r;
        });
    }
    UpdateInner(obj, db) {
        return __awaiter(this, void 0, void 0, function* () {
            delete obj._id;
            return new Promise((resolve, reject) => {
                db.update({ id: obj.id }, obj, { upsert: true }, (err, numReplaced, upsert) => {
                    if (err) {
                        reject(err);
                    }
                    else {
                        resolve(obj);
                    }
                });
            });
        });
    }
    GetEntity(tableName, id, db) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                db.findOne({ id: id }, (err, r) => {
                    if (err)
                        reject(err);
                    else
                        resolve(r);
                });
            });
        });
    }
    Delete(obj) {
        return __awaiter(this, void 0, void 0, function* () {
            let db = yield nedbPool_1.NeDBPool.Current.GetDBConnection(obj.toString(), this.config);
            let entity;
            if (this.transOn) {
                entity = yield this.GetEntity(obj.toString(), obj.id, db);
                entity.toString = obj.toString;
            }
            yield this.DeleteInner(obj, db);
            this.PushQuery("delete", entity);
            return true;
        });
    }
    DeleteInner(obj, db) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                db.remove({ id: obj.id }, {}, (err, numRemoved) => {
                    if (err)
                        reject(err);
                    else
                        resolve(true);
                });
            });
        });
    }
    BeginTranscation() {
        this.transOn = true;
    }
    Commit() {
        console.log("Commit this.transOn:", this.transOn);
        if (this.transOn) {
            this.transList = [];
            this.transOn = false;
        }
        ;
    }
    Query(qFn, tableName, queryMode, orderByFn, inqObj) {
        return __awaiter(this, void 0, void 0, function* () {
            if (queryMode == undefined || queryMode == null)
                queryMode = QueryMode.Normal;
            let db = yield nedbPool_1.NeDBPool.Current.GetDBConnection(tableName, this.config);
            let promise = new Promise((resolve, reject) => {
                let queryFn = {};
                if (qFn) {
                    queryFn = {
                        $where: function () {
                            try {
                                let r = true;
                                for (let i = 0; i < qFn.length; i++) {
                                    if (qFn[i] == null || qFn[i] == undefined)
                                        break;
                                    if (!qFn[i](this)) {
                                        r = false;
                                        break;
                                    }
                                }
                                return r;
                            }
                            catch (error) {
                                return false;
                            }
                        }
                    };
                }
                switch (queryMode) {
                    case QueryMode.Normal:
                        db.find(queryFn, (err, r) => {
                            if (err)
                                reject(err);
                            else
                                resolve(r);
                        });
                        break;
                    case QueryMode.Count:
                        db.count(queryFn, (err, r) => {
                            if (err)
                                reject(err);
                            else
                                resolve(r);
                        });
                        break;
                    case QueryMode.First:
                        db.findOne(queryFn, (err, r) => {
                            if (err)
                                reject(err);
                            else
                                resolve(r);
                        });
                        break;
                    case QueryMode.Contains:
                        let inq = {};
                        inq[inqObj.feildName] = {
                            $in: inqObj.value
                        };
                        db.find(inq, (err, r) => {
                            if (err)
                                reject(err);
                            else
                                resolve(r);
                        });
                        break;
                    default:
                        reject("未知的QueryMode!");
                        break;
                }
            });
            return promise;
        });
    }
    RollBack() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.transOn) {
                try {
                    for (let index = this.transList.length - 1; index >= 0; index--) {
                        let item = this.transList[index];
                        let db = yield nedbPool_1.NeDBPool.Current.GetDBConnection(item.entity.toString(), this.config);
                        switch (item.key) {
                            case "create":
                                yield this.DeleteInner(item.entity, db);
                                break;
                            case "update":
                                yield this.UpdateInner(item.entity, db);
                                break;
                            case "delete":
                                yield this.CreateInner(item.entity, db);
                                break;
                            default: throw "未知的回滚类型！";
                        }
                    }
                }
                catch (error) {
                    console.log("回滚失败", error);
                }
                finally {
                    this.transList = [];
                }
            }
        });
    }
    PushQuery(key, obj) {
        if (this.transOn) {
            this.transList.push({
                key: key,
                entity: obj
            });
        }
    }
}
exports.NeDBDataContext = NeDBDataContext;
var QueryMode;
(function (QueryMode) {
    QueryMode[QueryMode["Normal"] = 0] = "Normal";
    QueryMode[QueryMode["First"] = 1] = "First";
    QueryMode[QueryMode["Count"] = 2] = "Count";
    QueryMode[QueryMode["Contains"] = 3] = "Contains";
})(QueryMode = exports.QueryMode || (exports.QueryMode = {}));
//# sourceMappingURL=dataContextNeDB.js.map