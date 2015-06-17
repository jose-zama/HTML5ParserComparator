<%@taglib uri="http://www.springframework.org/tags" prefix="spring"%>
<%@taglib uri="http://www.springframework.org/tags/form" prefix="form"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>

<html>
<head>
<title>HTML5 report</title>
</head>
<body>

	<a href="index.html">Main Men&uacute;</a>

	<form:form method="POST" action="inputform.html"
		commandName="parserInput" class="form-horizontal" role="form">

		<div class="form-group">
			<label class="control-label col-sm-2" for="inputType">Type of
				input</label>
			<div class="col-sm-2">
				<form:select path="type" items="${inputTypeList}" id="inputType"
					class="form-control input-small" />
			</div>
		</div>
		<div class="form-group">
			<label class="control-label col-sm-2" for="value">Input:</label>
			<div class="col-sm-10">
				<form:input path="value" class="form-control" id="value"
					placeholder="Insert the URL or string here" />
			</div>
		</div>
		<div class="form-group">
			<div class="col-sm-offset-2 col-sm-10">
				<button type="submit" class="btn">Parse and
					compare!!!</button>
			</div>
		</div>
	</form:form>

</body>
</html>