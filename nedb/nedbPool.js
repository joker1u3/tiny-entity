"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments)).next());
    });
};
const dbOpenWorker_1 = require("./dbOpenWorker");
const Datastore = require("nedb");
class NeDBPool {
    constructor() {
        this.list = [];
    }
    GetDBConnection(tbName, config) {
        return __awaiter(this, void 0, void 0, function* () {
            let db = null;
            let connStr = config.FilePath + tbName + ".db";
            let item = this.list.find(x => x.connStr == connStr);
            if (item) {
                return item.db;
            }
            else {
                db = yield this.Open(connStr);
                this.list.push({ connStr: connStr, db: db });
                return db;
            }
        });
    }
    Open(connStr) {
        return new Promise((resolve, reject) => {
            let db = new Datastore({
                filename: connStr,
                inMemoryOnly: false,
                autoload: true,
                onload: (err) => {
                    if (err) {
                        if (err.errorType == "uniqueViolated")
                            reject(err);
                        else {
                            console.log("启动open task:" + connStr);
                            dbOpenWorker_1.OpenWorkerManager.Current.Task(new dbOpenWorker_1.DBOpenWorker({ path: connStr }, resolve));
                        }
                    }
                    else {
                        db.ensureIndex({ fieldName: 'id', unique: true }, (err) => {
                            if (err)
                                console.log("添加索引失败：", err);
                        });
                        resolve(db);
                    }
                }
            });
            db.persistence.setAutocompactionInterval(120 * 60 * 1000);
        });
    }
}
NeDBPool.Current = new NeDBPool();
exports.NeDBPool = NeDBPool;
//# sourceMappingURL=nedbPool.js.map