import tika
tika.initVM()
from tika import parser
parsed = parser.from_file('CS-25Amendment24.pdf')
print(parsed["metadata"])
#print(parsed["content"])