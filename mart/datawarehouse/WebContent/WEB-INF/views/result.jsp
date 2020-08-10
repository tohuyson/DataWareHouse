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

<style>
body {
	background-color: #83e59a;
}

.container {
	padding: 2em 10em;
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
	<div id="news" class="container">
			<form id="search-form" action="Control" method="post">
			<input type="text" list="" id="id_config" name="id_config"
				placeholder="insert id config " onkeyup="enableDisable(this)" /> <input
				id="btnSubmit" type="submit" name="" value="Submit">
		</form>
		<div class="wrapper">
			<h1 class="brand">
				<span>Log</span>
			</h1>
			<div class="log ">
				<table class="table table-bordered table-responsive">
			<thead>
				<h4>
					<b></b>
				</h4>
				<tr>
					<th>Id_config</th>
					<th>Name</th>
					<th>Status download</th>
					<th>Status staging</th>
					<th>Status warehouse</th>
				</tr>
			</thead>
			<tbody>
			
				<!-- lay danh sach mon hoc va hien thi len bang -->
				<c:forEach items="${list }" var="log">
					<tr>
						<td>${log.id_config}</td>
						<td>${log.name_file_local}</td>
						<td>${log.status_download}</td>
						<td>${log.status_stagging}</td>
						<td>${log.status_warehouse}</td>
						
						
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
