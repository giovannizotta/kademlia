FILENAME?="data/small_dataset.txt"
DEBUG?=0
NODES?=30
TIME?=10000
SEED?=420

main: help

clean:
	@rm -f logs.log
	@rm -r chord.pdf
	@rm -r kad.pdf

run_chord:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD

run_kad:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD

plot_chord:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht CHORD --plot True

plot_kad:
	@python3 main.py --nodes $(NODES) --max-time $(TIME) \
	--seed $(SEED) --dht KAD --plot True

help: 
	@python3 main.py --help
