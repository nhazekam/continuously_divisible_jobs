all: dependencies serial direct

serial: serial_sql serial_root
direct: direct_sql direct_sql_dyn # direct_root (broken)

job_drivers=  multicore_job_driver.py multithread_job_driver.py
shared= DivisibleJob.py DivisibleJobStats.py

dependencies : ${job_drivers} ${shared}

%.py : ../%.py
	cp $< $@

serial_sql: data uproot_env
	. envs/uproot/bin/activate
	python analysis_serial.py sql

serial_root: data uproot_env
	. envs/uproot/bin/activate
	python analysis_serial.py root

data:
	./get_data

SIZE = 2000000
QT = sql root
FIRST_SPLIT_COARSE = $(shell awk \
	'BEGIN {i = $(SIZE);  while (i > 2000) { print i; i=i/10 } }')
FIRST_SPLIT_FINE = $(shell awk \
	'BEGIN {i = $(SIZE);  while (i > 200) { print i " " i/2 " " i/4; i=i/10 } }')
REPEAT = 10

fine_coarse :  \
				$(foreach fs, ${FIRST_SPLIT_COARSE},  \
				$(foreach rep, ${REPEAT},  \
				$(foreach dyn, dyn ft,  \
                $(foreach t, ${QT}, \
					results/$(t)_$(fs)_$(rep)_$(dyn).time))))



fine_results :  \
				$(foreach fs, ${FIRST_SPLIT_FINE},  \
				$(foreach rep, ${REPEAT},  \
				$(foreach dyn, dyn ft,  \
                $(foreach t, ${QT}, \
					results/$(t)_$(fs)_$(rep)_$(dyn).time))))

# This creates and populates a lot of data files in the compiled directory
compiled/compiled.dat : 
	mkdir -p compiled
	perl proc_time.pl results/*.time

compiled/root.dat : compiled/compiled.dat
	(head -n 1 $< && grep root $<) > $@

compiled/sql.dat : compiled/compiled.dat
	(head -n 1 $< && grep sql $<) > $@

compiled/%_sorted.dat : compiled/%.dat
	(head -n 1 $< && tail -n +2 $< | sort -n -k2,2) > $@

compiled/%_dyn.dat : compiled/%.dat
	(head -n 1 $< && grep dyn $<) > $@

compiled/%_ft.dat : compiled/%.dat
	(head -n 1 $< && grep ft $<) > $@

# Run proc data prior to this
graphs/compare_dimuon.pdf : graph_comparison.plt compiled/sql_ft_sorted.dat compiled/root_ft_sorted.dat compiled/sql_dyn_sorted.dat compiled/root_dyn_sorted.dat
	mkdir -p graphs
	gnuplot -c $^ $@



results/%.time : uproot_env 
	mkdir -p results
	. envs/uproot/bin/activate
	(time python analysis_thread.py \
		$(subst _, , $*) \
	 ) &> results/$*.time


direct_sql_dyn: uproot_env
	. envs/uproot/bin/activate
	python analysis_thread.py sql 50000 1 dyn

direct_root_dyn: uproot_env
	. envs/uproot/bin/activate
	python analysis_thread.py root 50000 1 dyn


direct_sql: uproot_env
	. envs/uproot/bin/activate
	python analysis_direct.py sql 50000 1 ft

direct_root: uproot_env
	. envs/uproot/bin/activate
	python analysis_direct.py root 50000 1 ft

python_deps: setuptools logging numpy uproot matplotlib histogrammar

setuptools: uproot_env
	. envs/uproot/bin/activate
	pip install -U pip setuptools

logging: uproot_env
	. envs/uproot/bin/activate
	pip install --user logging

numpy: uproot_env
	. envs/uproot/bin/activate
	pip install --user numpy

histogrammar: uproot_env
	. envs/uproot/bin/activate
	pip install --user histogrammar

uproot: uproot_env
	. envs/uproot/bin/activate
	pip install --user uproot

matplotlib: uproot_env
	. envs/uproot/bin/activate
	pip install --user -I matplotlib==2.1.2

virtualenv:
	pip install --user virtualenv

uproot_env: envs/uproot
envs/uproot:
	virtualenv --system-site-packages envs/uproot

clean:
	rm -rf ${job_drivers}
	rm -rf ${shared}
	rm -rf output.json* output.png
	rm -rf DYJetsToLL_M.db DYJetsToLL_M.root
	rm -rf envs

