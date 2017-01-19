import { ContextConfig } from './dataContextNeDB';
import { OpenWorkerManager, DBOpenWorker } from './dbOpenWorker';
import Datastore = require("nedb");

export class NeDBPool {
    private list: {
        connStr: string;
        db: Datastore
    }[];
    private config: ContextConfig;
    constructor() {
        this.list = [];
    }
    static Current: NeDBPool = new NeDBPool();
    async GetDBConnection(tbName: string, config: ContextConfig): Promise<Datastore> {
        let db: Datastore = null;
        let connStr = config.FilePath + tbName + ".db";
        let item = this.list.find(x => x.connStr == connStr);

        if (item) {
            return item.db;
        }
        else {
            db = await this.Open(connStr);
            this.list.push({ connStr: connStr, db: db });
            return db;
        }
    }

    private Open(connStr: string): Promise<Datastore> {
        return new Promise((resolve, reject) => {
            let db = new Datastore({
                filename: connStr,
                inMemoryOnly: false,
                autoload: true,
                onload: (err: any) => {
                    if (err) {
                        if (err.errorType == "uniqueViolated") reject(err);
                        else {
                            console.log("启动open task:" + connStr);
                            OpenWorkerManager.Current.Task(new DBOpenWorker(
                                { path: connStr }, resolve
                            ));
                        }
                    }
                    else {
                        db.ensureIndex({ fieldName: 'id', unique: true }, (err) => {
                            if (err) console.log("添加索引失败：", err);
                        });
                        resolve(db);
                    }
                }
            });

            (<any>db).persistence.setAutocompactionInterval(120 * 60 * 1000);
        });
    }
}