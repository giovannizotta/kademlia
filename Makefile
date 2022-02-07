#!/bin/bash
NODES?=1000
TIME?=10000
SEED?=420
LEVEL?=INFO
RATE?=0.01
DATADIR?=res/data
PLOTDIR?=res/plots

main: help

clean:
	@rm -f *.log
	@rm -rf $(DATADIR)

prepare: clean
	@mkdir -p $(DATADIR)
	@mkdir -p $(PLOTDIR)

run_chord: prepare
	@echo "Running Chord with $(NODES) nodes for $(TIME) seconds, rate: $(RATE)"
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD --loglevel $(LEVEL) --rate $(RATE) --file $(DATADIR)/CHORD.json

run_kad: prepare
	@echo "Running Kad with $(NODES) nodes for $(TIME) seconds, rate: $(RATE)"
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD --loglevel $(LEVEL) --rate $(RATE) --file $(DATADIR)/KAD.json

plot_chord:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD --plot True --file $(DATADIR)/CHORD.json

plot_kad:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD --plot True --file $(DATADIR)/KAD.json

plot:
	@python3 plot.py
	@echo "Plots completed."

plots: run_kad run_chord plot

plot_arrival_rate: prepare
	@for rate in 0.01 0.02 0.05 0.1; do \
		echo "Rate: $$rate" ;\
		echo "Running Kad";\
		python3 main.py --nodes $(NODES) --max-time $(TIME) \
		--seed $(SEED) --dht KAD --loglevel $(LEVEL) --file $(DATADIR)/KAD_$$rate.json --rate $$rate; \
		echo "Running Chord";\
		python3 main.py --nodes $(NODES) --max-time $(TIME) \
			--seed $(SEED) --dht CHORD --loglevel $(LEVEL) --file $(DATADIR)/CHORD_$$rate.json --rate $$rate; \
	done
	@python3 plot.py --arrivals
	@echo "Arrivals plot completed."

asd:

help: 
	@python3 main.py --help
