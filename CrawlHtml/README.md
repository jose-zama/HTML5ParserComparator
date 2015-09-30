# CrawlHTML

## Usage

java -jar CrawlHtml.jar "parsername" "command" "CCfilepath"

Example

java -jar CrawlHtmlTool.jar -parse  "html5lib" "python html5libAdapter.py -f" "sample.warc.gz"

java -jar CrawlHtmlTool.jar -parse  "parse5" "node parse5.js -f" "sample.warc.gz"

java -jar CrawlHtmlTool.jar -parse  "MScParser" "java -jar MScParser.jar -f" "sample.warc.gz"

java -jar CrawlHtmlTool.jar -parse  "jsoup" "java -jar JsoupParser.jar -f" "sample.warc.gz"

java -jar CrawlHtmlTool.jar -parse "validatorNU" "java -jar validatorNuAdapter.jar -f" "sample.warc.gz"

java -jar CrawlHtmlTool.jar -parse  "AngleSharp" "AngleSharpAdapter.exe -f" "sample.warc.gz"
