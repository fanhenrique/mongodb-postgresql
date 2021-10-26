import sys
import subprocess
import argparse


DEFAULT_COUNT = 2

def main():

	parser = argparse.ArgumentParser(description='testes')
	parser.add_argument("--count", "-c", help="number of executions", default=DEFAULT_COUNT, type=int)
	
	args = parser.parse_args()

	cmd1 = "python3 postgres.py -c {}".format(args.count)
	subprocess.call(cmd1, shell=True)
	
	cmd2 = "python3 mongodb.py -c {}".format(args.count)
	subprocess.call(cmd2, shell=True)

if __name__ == '__main__':
	main()