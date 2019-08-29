
.PHONY: all clean clean_full proc_data graphs results_fine results_coarse archive_fine

#Reference base name and indices
RN=oak2.fa
REF=${RN} oak2.fa.amb oak2.fa.ann oak2.fa.bwt oak2.fa.pac oak2.fa.sa


#Sample SIZE 
size=10000

#Data Name
DN=test

#Query Types
QT = index-vf simple

# We don't want changes here to trigger data re-run automatically
JOB_DRIVERS=$(foreach t, multicore sequential local wq, $(t)_job_driver.py)
SHARED= BWA.py BWAQuery.py ${REF}
SHARED= DivisibleJobStats.py DivisibleJob.py VirtualFile.py bwa ${JOB_DRIVERS}

#FLAT Testing
FLAT_TEST = $(foreach t, ${QT}, results/${DN}_$(size)_ft_$(t)_1000.time)

#TLA Testing
LOCAL_TLA_TEST = $(foreach t, ${QT}, results/${DN}_$(size)_tla_$(t)_1000_100_5.time)

#TLA Testing
LOCAL_TLA_F_TEST = $(foreach t, ${QT}, results/${DN}_$(size)_tla_$(t)_1000_100_5_f.time)

#TLA Testing
LOCAL_TLA_S_TEST = $(foreach t, ${QT}, results/${DN}_$(size)_tla_$(t)_1000_100_5_s.time)

#TLA Testing
LOCAL_TLA_B_TEST = $(foreach t, ${QT}, results/${DN}_$(size)_tla_$(t)_1000_100_5_b.time)
WQ_TLA_B_TEST = $(foreach t, ${QT}, wq/${DN}_$(size)_tla_$(t)_1000_100_5_b.time)

all: test

test: ${SHARED} \
 ${FLAT_TEST} \
 ${LOCAL_TLA_TEST} \
 ${LOCAL_TLA_F_TEST} \
 ${LOCAL_TLA_S_TEST} \
 ${LOCAL_TLA_B_TEST} \
 ${WQ_TLA_B_TEST} \
 reference/${DN}_${size}.time
 
clean_test:
	rm ${FLAT_TEST} \
		${LOCAL_TLA_TEST} \
		${LOCAL_TLA_F_TEST} \
		${LOCAL_TLA_S_TEST} \
		${LOCAL_TLA_B_TEST} \
		${WQ_TLA_B_TEST} \
		reference/${DN}_${size}.time
 
software: ${SHARED}

./% : ../%
	cp $< $@

proc_data : current_data
	mkdir -p compiled
	perl proc_time.pl results/*.time

proc_data_wq : 
	mkdir -p compiled_wq
	perl proc_time_wq.pl wq/*.time

#  compiled/100000_tla_${ST}_%_second_index-vf_${LOCK}.dat \
# Run proc data prior to this
graphs/%.pdf : graph_results.plt compiled/%_index-vf.dat compiled/%_simple.dat
	mkdir -p graphs
	gnuplot -c $^ $@ $(shell awk \
	'BEGIN { n=split("$*",a,"_") ;  print (n>1 && a[3] eq "first") ? "second" : "first" ; }')


${DN}_10000_%.fq : ${DN}_10000.fq
	ln $< $@

# Just run bwa on data sample
reference/${DN}_%.sam reference/${DN}_%.time : ${DN}_%.fq ${RN} bwa
	mkdir -p reference
	(time ./bwa mem  ${RN} ${DN}_$*.fq -o reference/${DN}_$*.sam) &> reference/${DN}_$*.time

# Run test sample with specified arguments
results/${DN}_%.sam results/${DN}_%.time : ${DN}_%.fq 
	mkdir -p results
	(time python BWA.py ${RN} ${DN}_$*.fq results/${DN}_$*.sam  \
		$(subst _, , $*) \
	 ) &> results/${DN}_$*.time
	rm $<


# Run a test sampel with wq and arguments
wq/${DN}_%.sam wq/${DN}_%.debug wq/${DN}_%.tr wq/${DN}_%.stats wq/${DN}_%.time : ${DN}_%.fq 
	rm -rf $@
	mkdir -p wq
	(time python BWA_WQ.py ${RN} ${DN}_$*.fq wq/${DN}_$*.sam  \
		$(subst _, , $*) \
		-N cdj-test --port 9050 \
        -d all --debug-log wq/${DN}_$*.debug \
        --stats wq/${DN}_$*.stats \
        --trans wq/${DN}_$*.tr \
	 ) &> wq/${DN}_$*.time
	rm -f *.sam
	rm -f *.tar
	rm -f *.json

#Profiling Rules

profiles/%.pstats : BWAQuery.py BWA.py 
	mkdir -p profiles
	python -m cProfile -o $@ -s cumtime BWA_$*.py ${RN} ${DN}.fq $*/${DN}.sam

%.profile : %.pstats printStats.py
	python printStats.py $< > $@ 

# Software for analysis and comparison

bwa-src:
	git clone https://github.com/nhazekam/bwa bwa-src

bwa: bwa-src
	cd bwa-src; git checkout add_offset_limits;  make ; cp bwa ..

samtools:
	wget https://github.com/samtools/samtools/releases/download/1.9/samtools-1.9.tar.bz2
	tar -xjvf samtools-1.9.tar.bz2
	cd samtools-1.9; ./configure --prefix ${PWD} ; make ; make install ; cp samtools ..

bamUtil-src :
	git clone https://github.com/statgen/bamUtil.git bamUtil-src

bam: bamUtil-src
	cd bamUtil-src; make cloneLib; make ; cp bin/bam ..

#Clean up scripts

archive_%: BWA_%_results.tar.gz

BWA_fine_results.tar.gz : 
	tar czvf BWA_fine_results.tar.gz results/*.time compiled

clean:
	rm -f */*.sam
	rm -rf profiles
	rm -rf splits
	rm -f *_to_*.*
	rm -f *.pkl
	rm -f *.pyc
	rm -f *.json
	rm -f *_comp *_lines
	rm -f *virtual_index
	rm -f *_job_driver.py

clean_full : clean
	rm -f */test_*.time
	rm -rf bwa bwa-src
	rm -rf bam bamUtil-src libStatGen
	rm -rf samtools samtools-1.9*


