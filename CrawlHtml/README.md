# CrawlHTML

## Usage

java -jar CrawlHtml.jar "parsername" "command" "CCfilepath"

Example

java -jar CrawlHtml.jar "html5lib" "python /home/jose/HTML5ParserComparator/Parsers/html5lib/html5libAdapter.py -f" "sample.warc.gz"

java -jar CrawlHtml.jar "parse5" "node /home/jose/HTML5ParserComparator/Parsers/parse5/parse5.js -f" "sample.warc.gz"

java -jar CrawlHtmlTool.jar -parse  "MScParser" "java -jar /home/jose/HTML5ParserComparator/Parsers/MScParser/MScParser.jar -f" "sample.warc.gz"

java -jar CrawlHtmlTool.jar -parse  "jsoup" "java -jar /home/jose/HTML5ParserComparator/Parsers/jsoup/JsoupParser.jar -f" "sample.warc.gz"

java -jar CrawlHtml.jar "validatorNU" "java -jar /home/jose/HTML5ParserComparator/Parsers/validatorNu/validatorNuAdapter.jar -f" "sample.warc.gz"

java -jar CrawlHtmlTool.jar -parse "validatorNU" "java -jar /home/jose/HTML5ParserComparator/Parsers/validatorNu/validatorNuAdapter.jar -f" "sample.warc.gz"

