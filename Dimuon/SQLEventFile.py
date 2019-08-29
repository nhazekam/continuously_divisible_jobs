from EventFile import EventFile
import sqlite3

class SQLEventFile(EventFile):
    def __init__(self, root_file, repeat = 1):
        self.sql_db  = sqlite3.connect(root_file)
        self.sql_cur = self.sql_db.cursor()
        self.repeat  = repeat
        super(SQLEventFile, self).__init__(root_file)

    @property
    def maxend(self):
        self.sql_cur.execute('SELECT COUNT(DISTINCT event) FROM Muon')
        return self.repeat * self.sql_cur.fetchone()[0]

    def events_at(self, columns, start=0, count=None):
        limit,offset = '',''
        if count:
            limit = "LIMIT {}".format(count)
        if start:
            offset = "OFFSET {}".format(start)

        for n in range(self.repeat):
            self.sql_cur.execute('SELECT {} FROM Muon {} {}'.format(','.join(['event'] + columns), limit, offset))
            last = None

            rows = [[] for i in columns]
            for row in self.sql_cur:
                if last:
                    if last[0] == row[0]: #event key
                        for i in range(0,len(columns)):
                            rows[i].append(row[i+1]) #+1 to skip event key
                    else:
                        if len(rows[0]) > 1:
                            yield rows
                            rows = [[] for i in columns]
                            for i in range(0,len(columns)):
                                rows[i].append(row[i+1]) #+1 to skip event key
                else:
                    for i in range(len(columns)):
                        rows[i].append(row[i+1])
                last = row

