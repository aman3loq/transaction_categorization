class Streamer(object):

	def __init__(self, file_path):
		self.file_path = file_path

	def __iter__(self):
		for line in open(self.file_path, 'r'):
			#print(line.strip().split('|')[1].split(','))
			yield line.strip().split('|')[1].split(',')

	def line_count(self):
		count = 0
		for line in open(self.file_path, 'r'):
			count += 1
		return count




if __name__ == '__main__':
	file_path = 'setup.py'
	S = Streamer(file_path)
	for line in S:
		print (line)


		
