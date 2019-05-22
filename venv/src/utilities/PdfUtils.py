from tika import parser

parsed_pdf = parser.from_file('http://iblocks-rg-publication.s3-website-eu-west-1.amazonaws.com/pink_pages.pdf')

print(parsed_pdf.keys())

# print(parsed_pdf['content'])
print(parsed_pdf['metadata'].get('Last-Modified'))

content = parsed_pdf['content']

content = content.split('\n')

cnt = 0
for line in content:
    line.replace('\t', 'ABC')
    print(line)
    cnt += 1
    if cnt == 50:
        exit()
