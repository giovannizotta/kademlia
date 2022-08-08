#/bin/bash
NODES?=100
TIME?=1000
SEED?=420
ALPHA?=3
K?=5
LEVEL?=INFO
RATE?=0.1
IMGDIR?=img
LOGDIR?=logs
DATADIR?=res/data
PLOTDIR?=res/plots
EXT?=pdf
RATES?=0.01 0.02 0.05 0.1

main: help

clean:
	@rm -f $(LOGDIR)/*.log

prepare: clean
	@mkdir -p $(IMGDIR)
	@mkdir -p $(LOGDIR)
	@mkdir -p $(DATADIR)
	@mkdir -p $(PLOTDIR)

run_chord: prepare
	@echo "Running Chord with $(NODES) nodes for $(TIME) seconds, rate: $(RATE)"
	@simulate --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD --loglevel $(LEVEL) --rate $(RATE) --datadir $(DATADIR)

run_kad: prepare
	@echo "Running Kad with $(NODES) nodes for $(TIME) seconds, rate: $(RATE)"
	@simulate --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD --loglevel $(LEVEL) --alpha $(ALPHA) -k $(K) \
	--rate $(RATE) --datadir $(DATADIR)

plot_chord:
	@simulate --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD --plot True --datadir $(DATADIR)

plot_kad:
	@simulate --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD --plot True 

plot:
	@plot --nodes $(NODES) --time $(TIME) --singlerate $(RATE)
	@echo "Plots completed."

plots: run_kad run_chord plot

plot_network: plot_chord plot_kad

plot_arrival_rate: prepare
	@for rate in $(RATES); do \
		echo "Rate: $$rate" ;\
		echo "Running Kad";\
		simulate --nodes $(NODES) --max-time $(TIME) \
		--seed $(SEED) --dht KAD --loglevel $(LEVEL) --file $(DATADIR)/KAD_$(NODES)_$(TIME)_$$rate.json --rate $$rate; \
		echo "Running Chord";\
		simulate --nodes $(NODES) --max-time $(TIME) \
			--seed $(SEED) --dht CHORD --loglevel $(LEVEL) --file $(DATADIR)/CHORD_$(NODES)_$(TIME)_$$rate.json --rate $$rate; \
	done
	@plot --arrivals --nodes $(NODES) --time $(TIME) --rates $(RATES)
	@echo "Arrivals plot completed."

help: 
	@simulate --help
