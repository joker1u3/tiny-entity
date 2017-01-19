import Datastore = require("nedb");
import { DBOpenWorker, OpenWorkerManager } from "./dbOpenWorker";
import { IEntityObject, IDataContext } from '../tinyDB';
import { NeDBPool } from './nedbPool';
var dbconfig;

export class NeDBDataContext implements IDataContext {
    private nedb: Datastore;
    private config: ContextConfig;
    private transOn: boolean;
    private transList: TransQuery[] = [];
    constructor(config: ContextConfig) {
        this.config = config;
        dbconfig = config;
        if (!config.IsMulitTabel) {
            this.nedb = new Datastore(config.FilePath + config.DBName);
            this.nedb.loadDatabase();
        }
    }

    async Create(obj: IEntityObject): Promise<Object> {
        delete (obj as any).ctx;
        try {
            let db = await NeDBPool.Current.GetDBConnection(obj.toString(), this.config);
            let r = await this.CreateInner(obj, db);
            this.PushQuery("create", { id: obj.id });
            return r;
        }
        catch (err) {
            if (err.errorType == "uniqueViolated") {
                throw { code: -101, message: "插入失败：重复的主键id" };
            } else {
                throw err;
            }
        }
    }
    private async CreateInner(obj: IEntityObject, db: Datastore): Promise<any> {
        return new Promise((resolve, reject) => {
            db.insert(obj, (err, r) => {
                if (err) reject(err);
                else {
                    resolve(r);
                }
            });
        });
    }

    async UpdateRange(list: [IEntityObject]) {
        let entityList = [];
        for (let item of list) {
            let v = await this.Update(item);
            entityList.push(v);
        }

        return entityList;
    }

    async Update(obj: IEntityObject) {
        delete (obj as any).ctx;
        let db = await NeDBPool.Current.GetDBConnection(obj.toString(), this.config);
        let entity;
        if (this.transOn) {
            entity = await this.GetEntity(obj.toString(), obj.id, db);
            entity.toString = obj.toString;
        }
        let r = await this.UpdateInner(obj, db);
        this.PushQuery("update", entity);
        return r;
    }

    private async UpdateInner(obj: IEntityObject, db: Datastore) {
        delete (<any>obj)._id;
        return new Promise((resolve, reject) => {
            db.update({ id: obj.id }, obj, { upsert: true }, (err, numReplaced: number, upsert) => {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(obj);
                }
            });
        })
    }

    private async GetEntity(tableName: string, id, db: Datastore) {
        return new Promise((resolve, reject) => {
            db.findOne({ id: id }, (err, r) => {
                if (err) reject(err);
                else resolve(r);
            });
        });
    }

    async Delete(obj: IEntityObject): Promise<boolean> {
        let db = await NeDBPool.Current.GetDBConnection(obj.toString(), this.config);
        let entity;
        if (this.transOn) {
            entity = await this.GetEntity(obj.toString(), obj.id, db);
            entity.toString = obj.toString;
        }
        await this.DeleteInner(obj, db);
        this.PushQuery("delete", entity);
        return true;
    }
    private async DeleteInner(obj: IEntityObject, db: Datastore) {
        return new Promise<boolean>((resolve, reject) => {
            db.remove({ id: obj.id }, {}, (err, numRemoved) => {
                if (err) reject(err);
                else resolve(true);
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
        };
    }
    async Query(qFn: [((p) => Boolean)], tableName: string, queryMode?: QueryMode, orderByFn?, inqObj?): Promise<any> {
        if (queryMode == undefined || queryMode == null) queryMode = QueryMode.Normal;
        let db = await NeDBPool.Current.GetDBConnection(tableName, this.config);
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
                        } catch (error) {
                            return false;
                        }
                    }
                }
            }
            switch (queryMode) {
                case QueryMode.Normal:
                    db.find(queryFn, (err, r) => {
                        if (err) reject(err);
                        else resolve(r);
                    });
                    break;
                case QueryMode.Count:
                    db.count(queryFn, (err, r) => {
                        if (err) reject(err);
                        else resolve(r);
                    });
                    break;
                case QueryMode.First:
                    db.findOne(queryFn, (err, r) => {
                        if (err) reject(err);
                        else resolve(r);
                    });
                    break;
                case QueryMode.Contains:
                    let inq = {};
                    inq[inqObj.feildName] = {
                        $in: inqObj.value
                    }
                    db.find(inq, (err, r) => {
                        if (err) reject(err);
                        else resolve(r);
                    });
                    break;

                default: reject("未知的QueryMode!"); break;
            }
        });

        return promise;
    }

    async RollBack() {
        if (this.transOn) {
            try {
                for (let index = this.transList.length - 1; index >= 0; index--) {
                    let item = this.transList[index];
                    let db = await NeDBPool.Current.GetDBConnection(item.entity.toString(), this.config);
                    switch (item.key) {
                        case "create":
                            await this.DeleteInner(item.entity, db);
                            break;
                        case "update":
                            await this.UpdateInner(item.entity, db);
                            break;
                        case "delete":
                            await this.CreateInner(item.entity, db);
                            break;
                        default: throw "未知的回滚类型！";
                    }
                }
            } catch (error) {
                console.log("回滚失败", error);
            }
            finally {
                this.transList = [];
            }
        }
    }
    private PushQuery(key, obj) {
        if (this.transOn) {
            this.transList.push({
                key: key,
                entity: obj
            });
        }
    }
}

export enum QueryMode {
    Normal,
    First,
    Count,
    Contains
}
export interface ContextConfig {
    IsMulitTabel?: boolean;
    FilePath: string;
    DBName: string;
    timestampData?: boolean;
}
interface TransQuery {
    key: string;
    entity: IEntityObject;
}