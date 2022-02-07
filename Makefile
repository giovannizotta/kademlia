#!/bin/bash
NODES?=1000
TIME?=10000
SEED?=420
LEVEL?=INFO
RATE?=0.01

main: help

clean:
	@rm -f logs.log
	@rm -f chord.pdf
	@rm -f kad.pdf
	@rm -f kad.png
	@rm -f chord.png
	@rm -f *.png
	@rm -f *.pdf
	@rm -f *.json
	@rm -f *.log

run_chord:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD --loglevel $(LEVEL) --file CHORD.json --rate $(RATE)

run_kad:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD --loglevel $(LEVEL) --file KAD.json --rate $(RATE)

plot_chord:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD --plot True --file CHORD.json

plot_kad:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD --plot True --file KAD.json

plots:
	@echo "Running Chord with $(NODES) nodes for $(TIME) seconds"
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD --loglevel $(LEVEL) --file CHORD.json
	@echo "Running Kad with $(NODES) nodes for $(TIME) seconds"
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD --loglevel $(LEVEL) --file KAD.json
	@python3 plot.py
	@echo "Plots completed."

plot_arrival_rate:
	@for rate in 0.01 0.02 0.05 0.1; do \
		echo "Rate: $$rate" ;\
		echo "Running Kad";\
		python3 main.py --nodes $(NODES) --max-time $(TIME) \
		--seed $(SEED) --dht KAD --loglevel $(LEVEL) --file KAD_$$rate.json --rate $$rate; \
		echo "Running Chord";\
		python3 main.py --nodes $(NODES) --max-time $(TIME) \
			--seed $(SEED) --dht CHORD --loglevel $(LEVEL) --file CHORD_$$rate.json --rate $$rate; \
	done
	@python3 plot.py --arrivals
	@echo "Arrivals plot completed."

asd:

help: 
	@python3 main.py --help
