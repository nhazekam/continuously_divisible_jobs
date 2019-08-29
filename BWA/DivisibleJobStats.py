

class  JobStat(object):
    def __init__(self, size, fit):
        self.size = size
        self.fit = fit

    @staticmethod
    def average(orig, new, members):
        a_size = ((orig.size * members) + new.size)/(members+1)
        a_fit = ((orig.fit * members) + new.fit)/(members+1)
        return JobStat(a_size, a_fit)
         
class JobStatHillClimb(object):
    def __init__(self, size, seed):
        self.current = seed
        self.fit = 0
        self.candidates = []
        self.stats = {}
        self.bucket_size = 1
        self.step = max(int(size/100), 1)
        self.accel = 1.2
        self.add_candidates()

    def add_candidates(self):
        moves = [0, -self.accel, self.accel, -1/self.accel, 1/self.accel]
        for i in moves:
            print "Candidates: {}".format(int(self.current + (self.step * i)))
            self.candidates.append(max(int(self.current + (self.step * i)), 1))

    def update_model(self, stat):
        bucket = int(stat.size/self.bucket_size)
        (size, prev) = self.stats.get(bucket, (0, None))
        if prev is None:
            updated = stat
            print "Added bucket {} : size {}".format(bucket, stat.size)
        else:
            updated = JobStat.average(prev, stat, size)
            print "Updated bucket {} : size {} : members {}".format(bucket, stat.size, size)
        self.stats[bucket] = (size + 1, updated)

    def find_best(self):
        if len(self.stats) < 3: # minimum sample size
            print "Length of stats {}".format(len(self.stats))
            return
        best_size = None
        best_fit = self.fit
        print "Length of stats {}".format(len(self.stats))
        for bucket, (members, stat) in self.stats.items():
            print "Comp {} > {}".format(stat.fit, best_fit)
            if stat.fit > best_fit:
                best_fit = stat.fit
                best_size = stat
        if best_size:
            print "Best {} {} {}".format(self.current, best_fit, best_size.size)
            diff_step = abs(self.current - best_size.size)
            if self.step == 0:
                self.step = diff_step
            else:
                rate_change = (diff_step+0.0)/self.step
                self.step = int(self.step*rate_change)
            self.current = best_size.size
            self.fit = best_size.fit
        else:
            self.step = int(self.step * 0.9)

    def get_next(self):
        if len(self.candidates) == 0:
            self.find_best()
            self.add_candidates()
        return self.candidates.pop()
        
class JobStatFixed(object):
    def __init__(self, size, seed):
        self.current = seed

    def update_model(self, stat):
        return

    def get_next(self):
        return self.current
        
