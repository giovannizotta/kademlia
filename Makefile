#!/bin/bash
NODES?=1000
TIME?=10000
SEED?=420
LEVEL?=INFO

main: help

clean:
	@rm -f logs.log
	@rm -r chord.pdf
	@rm -r kad.pdf
	@rm -r kad.png
	@rm -r chord.png

run_chord:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD --loglevel $(LEVEL) --file CHORD.data

run_kad:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD --loglevel $(LEVEL) --file KAD.data

plot_chord:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD --plot True --file CHORD.data

plot_kad:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD --plot True --file KAD.data

plots:
	@echo "Running Chord with $(NODES) nodes for $(TIME) seconds"
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD --loglevel $(LEVEL) --file CHORD.data
	@echo "Running Kad with $(NODES) nodes for $(TIME) seconds"
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD --loglevel $(LEVEL) --file KAD.data
	@python3 plot.py
	@echo "Plots completed."

plot_arrival_rate:
	@for rate in 0.01 0.02 0.05 0.1; do \
		echo "Rate: $$rate" ;\
		echo "Running Kad";\
		python3 main.py --nodes $(NODES) --max-time $(TIME) \
		--seed $(SEED) --dht KAD --loglevel $(LEVEL) --file KAD_$$rate.data --rate $$rate; \
		echo "Running Chord";\
		python3 main.py --nodes $(NODES) --max-time $(TIME) \
			--seed $(SEED) --dht CHORD --loglevel $(LEVEL) --file CHORD_$$rate.data --rate $$rate; \
	done
	@python3 plot.py --arrivals
	@echo "Arrivals plot completed."

asd:

help: 
	@python3 main.py --help
