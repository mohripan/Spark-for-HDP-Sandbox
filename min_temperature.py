from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('min_temperature')
sc = SparkContext(conf = conf)

def parse_line(line):
    fields = line.split(',')
    stationID = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entry_type, temperature)

lines = sc.textFile('1800.csv')
parsed_lines = lines.map(parse_line)
min_temps = parsed_lines.filter(lambda x: 'TMAX' in x[1])
station_temps = min_temps.map(lambda x: (x[0], x[2]))
min_temps = station_temps.reduceByKey(lambda x, y: max(x, y))
results = min_temps.collect()

for result in results:
    print(f'{result[0]} {result[1]:.2f}F')