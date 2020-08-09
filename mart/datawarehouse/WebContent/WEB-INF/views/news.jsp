<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="sql" uri="http://java.sun.com/jsp/jstl/sql"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>

<%@page import="com.datawarehouse.dao.Management"%>
<%@page import="com.datawarehouse.connect.DBConnect"%>
<%@page import="com.datawarehouse.model.Log"%>
<%@page import="java.util.ArrayList"%>
<%@page import="java.sql.Statement"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.Connection"%>
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="X-UA-Compatible" content="ie=edge">
<title>Log</title>
<link rel="stylesheet"
	href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css"
	integrity="sha384-9aIt2nRpC12Uk9gS9baDl411NQApFmC26EwAOH8WgZl5MYYxFfc+NcPb1dKGj7Sk"
	crossorigin="anonymous">

<!-- <link -->
<!-- 	href="https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css" -->
<!-- 	rel="stylesheet" -->
<!-- 	integrity="sha384-wvfXpqpZZVQGK6TAh5PVlGOfQNHSoD2xbE+QkPxCAFlNEevoEH3Sl0sibVcOQVnN" -->
<!-- 	crossorigin="anonymous"> -->
<!-- <link rel="stylesheet" -->
<!-- 	href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/3.5.2/animate.css" /> -->

<script
	src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js"></script>
<script>
	$(document).ready(function() {
							setInterval(function(){					
											$.ajax({
														url : '${pageContext.request.contextPath }/Update',
														type : 'POST',
														dataType : 'json',
														headers : {
															Accept : "application/json; charset=utf-8",
															"Content-Type" : "application/json; charset=utf-8"
														},
														success : function(data) {
															// 					$('.news').html('New ' + data);
															console.log(data);
															var product = $
																	.parseJSON(JSON
																			.stringify(data));
															var s = '';
															s += '<table class="table table-bordered table-responsive">'
															s += '<tr><th>Id</th>'
															s += '<th>Id config</th>'
															s += '<th>Status download</th>'
															s += '<th>Date time download</th>'
															s += '<th>Local path</th>'
															s += '<th>Name file local</th>'
															s += '<th>Extension</th>'
															s += '<th>Status staging</th>'
															s += '<th>Date time staging</th>'
															s += '<th>Load row staging</th>'
															s += '<th>Record end</th>'
															s += '<th>Status warehouse</th>'
															s += '<th>Date time warehouse</th>'
															s += '<th>Load row warehouse</th></tr>'
															for (var i = 0; i < product.length; i++) {
																s += '<tr><td> '
																		+ product[i].id
																		+ '</td>';
																s += '<td> '
																		+ product[i].id_config
																		+ '</td>';
																s += '<td> '
																		+ product[i].status_download
																		+ '</td>';
																s += '<td> '
																		+ product[i].date_time_download
																		+ '</td>';
																s += '<td> '
																		+ product[i].local_path
																		+ '</td>';
																s += '<td> '
																		+ product[i].name_file_local
																		+ '</td>';
																s += '<td> '
																		+ product[i].extension
																		+ '</td>';
																s += '<td> '
																		+ product[i].status_stagging
																		+ '</td>';
																s += '<td> '
																		+ product[i].date_time_staging
																		+ '</td>';
																s += '<td> '
																		+ product[i].load_row_stagging
																		+ '</td>';
																s += '<td> '
																		+ product[i].status_warehouse
																		+ '</td>';
																s += '<td> '
																		+ product[i].date_time_warehouse
																		+ '</td>';
																s += '<td> '
																		+ product[i].load_row_warehouse
																		+ '</td>';
																s += '<td> '
																	+ product[i].created_at
																	+ '</td>';
																s += '<td> '
																	+ product[i].updated_at
																	+ '</td></tr>';
															}
// 															s += '</table>'
															$('.news').html(s);
														},
													});
							},10000);
					});
</script>
<style>
body {
	background-color: #83e59a;
}

.fluid-container {
	padding: 3em;
}

.wrapper {
	padding: 3em;
	background-color: white;
	box-shadow: 0 0 20px 0 rgba(72, 94, 116, 0.7);
}

.bg-light {
	background-color: white !important;
}
</style>
</head>
<body>
<!-- 	<button class="test">OK</button> -->
	<nav class="navbar navbar-expand-lg navbar-light bg-light">
		<div class="collapse navbar-collapse" id="navbarNav">
			<ul class="navbar-nav">
				<li class="nav-item "><a class="nav-link" href="index.jsp">Home
						<span class="sr-only">(current)</span>
				</a></li>
				<li class="nav-item"><a class="nav-link" href="Config">Config</a>
				</li>
				<li class="nav-item active"><a class="nav-link" href="New">Logs</a>
				</li>
			</ul>
		</div>
	</nav>
	<div id="news" class="fluid-container">
			<form id="search-form" action="Control" method="post">
			<input type="text" list="dsTourName" id="id_config" name="id_config"
				placeholder="insert id confid " onkeyup="enableDisable(this)" /> <input
				id="btnSubmit" type="submit" name="" value="Submit">
		</form>
		<div class="wrapper">
			<h1 class="brand">
				<span>Log</span>
			</h1>
			<div class="log ">
				<table class="table table-bordered table-responsive news">
					<thead>
						<tr>
							<th>Id</th>
							<th>Id config</th>
							<th>Status download</th>
							<th>Date time download</th>
							<th>Local path</th>
							<th>Name file local</th>
							<th>Extension</th>
							<th>Status staging</th>
							<th>Date time staging</th>
							<th>Load row staging</th>
							<th>Status warehouse</th>
							<th>Date time warehouse</th>
							<th>Load row warehouse</th>
							<th>Created</th>
							<th>Updated</th>
						</tr>
					</thead>
					<tbody class="news">
						<c:forEach items="${list }" var="news">
							<tr>
								<td>${news.id }</td>
								<td>${news.id_config }</td>
								<td>${news.status_download }</td>
								<td>${news.date_time_download }</td>
								<td>${news.local_path }</td>
								<td>${news.name_file_local }</td>
								<td>${news.extension }</td>
								<td>${news.status_stagging }</td>
								<td>${news.date_time_staging }</td>
								<td>${news.load_row_stagging }</td>
								<td>${news.status_warehouse }</td>
								<td>${news.date_time_warehouse }</td>
								<td>${news.load_row_warehouse }</td>
								<td>${news.created_at }</td>
								<td>${news.updated_at }</td>
							</tr>
						</c:forEach>
							
					</tbody>
				</table>

			</div>
		</div>
	</div>

	<!-- 	<script src="https://code.jquery.com/jquery-3.5.1.slim.min.js" -->
	<!-- 		integrity="sha384-DfXdz2htPH0lsSSs5nCTpuj/zy4C+OGpamoFVy38MVBnE+IbbVYUew+OrCXaRkfj" -->
	<!-- 		crossorigin="anonymous"></script> -->
	<script
		src="https://cdn.jsdelivr.net/npm/popper.js@1.16.0/dist/umd/popper.min.js"
		integrity="sha384-Q6E9RHvbIyZFJoft+2mJbHaEWldlvI9IOYy5n3zV9zzTtmI3UksdQRVvoxMfooAo"
		crossorigin="anonymous"></script>
	<script
		src="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/js/bootstrap.min.js"
		integrity="sha384-OgVRvuATP1z7JjHLkuOU7Xw704+h835Lr+6QL9UvYjZE3Ipu6Tp75j7Bh/kR0JKI"
		crossorigin="anonymous"></script>
</body>
</html>
