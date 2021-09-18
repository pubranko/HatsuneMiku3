import sys
from scrapy.cmdline import execute
from scrapy.utils.project import get_project_settings


args = sys.argv
#args 

#execute(argv=['scrapy','crawl', args[1]])
execute(argv=args[1:])