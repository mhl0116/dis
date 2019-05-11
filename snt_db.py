import sqlite3

class SNTDBInterface():
    
    def __init__(self, fname="main.db"):

        self.connection = sqlite3.connect(fname, check_same_thread=False)

        self.cursor = self.connection.cursor()
        self.key_types = [
                ("sample_id", "INTEGER PRIMARY KEY"),
                ("timestamp", "INTEGER"),
                ("sample_type", "VARCHAR(30)"),
                ("twiki_name", "VARCHAR(60)"),
                ("dataset_name", "VARCHAR(250)"),
                ("location", "VARCHAR(300)"),
                ("filter_type", "VARCHAR(20)"),
                ("nevents_in", "INTEGER"),
                ("nevents_out", "INTEGER"),
                ("filter_eff", "FLOAT"),
                ("xsec", "FLOAT"),
                ("kfactor", "FLOAT"),
                ("gtag", "VARCHAR(40)"),
                ("cms3tag", "VARCHAR(40)"),
                ("baby_tag", "VARCHAR(40)"),
                ("analysis", "VARCHAR(30)"),
                ("assigned_to", "VARCHAR(20)"),
                ("comments", "VARCHAR(100)"),
                ]

    def drop_table(self):
        self.cursor.execute("drop table if exists sample")

    def make_table(self):
        sql_cmd = "CREATE TABLE sample (%s)" % ",".join(["%s %s" % (key, typ) for (key, typ) in self.key_types])
        self.cursor.execute(sql_cmd)

    def load_from_csv(self,fname):
        import csv
        self.drop_table()
        self.make_table()
        with open(fname,'r') as fin:
            # csv.DictReader uses first line in file for column headings by default
            dr = csv.DictReader(fin) # comma is default delimiter
            for d in dr:
                self.do_insert_dict(d)
        self.connection.commit()

    def export_to_csv(self,fname):
        import csv
        self.cursor.execute('SELECT * FROM sample')
        with open(fname,'w') as fhout:
          csv_out = csv.writer(fhout)
          csv_out.writerow([d[0] for d in self.cursor.description])
          for result in self.cursor:
              csv_out.writerow(result)

    def make_val_str(self, vals):
        return map(lambda x: '"%s"' % x if type(x) in [str,unicode] else str(x), vals)

    def do_insert_dict(self, d):
        # provide a dict to insert into the table
        keys, vals = zip(*d.items())
        key_str = ",".join(keys)
        val_str = ",".join(self.make_val_str(vals))
        sql_cmd = "insert into sample (%s) values (%s);" % (key_str, val_str)
        self.cursor.execute(sql_cmd)

    def do_update_dict(self, d, idx):
        # provide a dict and index to update
        keys, vals = zip(*d.items())
        val_strs = self.make_val_str(vals)
        set_str = ",".join(map(lambda (x,y): "%s=%s" % (x,y), zip(keys, val_strs)))
        sql_cmd = "update sample set %s where sample_id=%i" % (set_str, idx)
        self.cursor.execute(sql_cmd)

    def do_delete_dict(self, d, idx):
        # provide a dict and index to update
        sql_cmd = "delete from sample where sample_id=%i" % (idx)
        self.cursor.execute(sql_cmd)

    def is_already_in_table(self, d):
        # provide a dict and this will use appropriate keys to see if it's already in the database
        # this returns an ID (non-zero int) corresponding to the row matching the dict
        dataset_name, sample_type, cms3tag = d.get("dataset_name",""), d.get("sample_type",""), d.get("cms3tag","")
        baby_tag, analysis = d.get("baby_tag",""), d.get("analysis","")
        if baby_tag or analysis:
            sql_cmd = "select sample_id from sample where dataset_name=? and sample_type=? and cms3tag=? and baby_tag=? and analysis=? limit 1"
            self.cursor.execute(sql_cmd, (dataset_name, sample_type, cms3tag, baby_tag, analysis))
        else:
            sql_cmd = "select sample_id from sample where dataset_name=? and sample_type=? and cms3tag=? limit 1"
            self.cursor.execute(sql_cmd, (dataset_name, sample_type, cms3tag))
        return self.cursor.fetchone()

    def read_to_dict_list(self, query):
        # return list of sample dictionaries
        self.cursor.execute(query)
        col_names = [e[0] for e in self.cursor.description]
        toreturn = []
        for r in self.cursor.fetchall():
            toreturn.append( dict(zip(col_names, r)) )
        return toreturn

    def update_sample(self, d):
        # provide dictionary, and this will update sample if it already exists, or insert it

        if not d: return False
        if self.unknown_keys(d): return False

        # totally ignore the sample_id
        if "sample_id" in d: del d["sample_id"]
        already_in = self.is_already_in_table(d)
        if already_in: self.do_update_dict(d, already_in[0])
        else: self.do_insert_dict(d)
        self.connection.commit()
        return True

    def delete_sample(self, d):
        # provide dictionary, and this will update sample if it already exists, or insert it

        if not d: return False
        if self.unknown_keys(d): return False

        # totally ignore the sample_id
        if "sample_id" in d: del d["sample_id"]
        already_in = self.is_already_in_table(d)
        if already_in:
            self.do_delete_dict(d, already_in[0])
            self.connection.commit()
            return True
        return False


    def fetch_samples_matching(self, d):
        # provide dictionary and this will find samples with matching key-value pairs

        if not d: return []
        if self.unknown_keys(d): return []

        # sanitize wildcards
        for k in d:
            if type(d[k]) in [str,unicode] and "*" in d[k]:
                d[k] = d[k].replace("*","%")

        keys, vals = zip(*d.items())
        val_strs = self.make_val_str(vals)

        def need_wildcard(y):
            return ("%" in y) or ("[" in y) or ("]" in y)

        set_str = " and ".join(map(lambda (x,y): "%s %s %s" % (x,'like' if need_wildcard(y) else '=', y), zip(keys, val_strs)))
        sql_cmd = "select * from sample where %s" % (set_str)
        return self.read_to_dict_list(sql_cmd)

    def unknown_keys(self, d):
        # returns True if there are unrecognized keys
        unknown_keys = list(set(d.keys()) - set([kt[0] for kt in self.key_types]))
        if len(unknown_keys) > 0:
            # print "I don't recognize the keys: %s" % ", ".join(unknown_keys)
            return True
        else: return False


    def close(self):
        self.connection.close()


if __name__=='__main__':
    pass
