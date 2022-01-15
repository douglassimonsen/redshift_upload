copy {file_destination} {columns} 
from '{source}'
credentials 'aws_access_key_id={access};aws_secret_access_key={secret}'
csv
NULL ''
FILLRECORD

BZIP2