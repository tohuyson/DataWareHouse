<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="sql" uri="http://java.sun.com/jsp/jstl/sql"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>

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
<style>
.container {
	padding: 1em 15em;
}

.wrapper {
	padding: 1em 4em;
	background-color: white;
	box-shadow: 0 0 20px 0 rgba(72, 94, 116, 0.7);
	border-radius: 10px;
}
body{
	background-color: #83e59a;
}
.btn-success{
	background-color: #1f6c2fdb;
	border: none;
}
.bg-light{
	background-color: white !important;
}
</style>
</head>
<body>
<nav class="navbar navbar-expand-lg navbar-light bg-light">
  <div class="collapse navbar-collapse" id="navbarNav">
    <ul class="navbar-nav">
      <li class="nav-item ">
        <a class="nav-link" href="index.jsp">Home <span class="sr-only">(current)</span></a>
      </li>
      <li class="nav-item active">
        <a class="nav-link" href="Config">Config</a>
      </li>
      <li class="nav-item">
        <a class="nav-link" href="New">Logs</a>
      </li>
    </ul>
  </div>
</nav>
	<div class="container">
<!-- 		<form id="search-form" action="Control" method="post"> -->
<!-- 			<input type="text" list="dsTourName" id="id_config" name="id_config" -->
<!-- 				placeholder="insert id confid " onkeyup="enableDisable(this)" /> <input -->
<!-- 				id="btnSubmit" type="submit" name="" value="Submit"> -->
<!-- 		</form> -->
		<div class="config">
			<div class="wrapper">
			<h3 class="brand">
				<span>Config</span>
			</h3>
				<form class="form" action="Insert" method="post">
					<div class="form-group">
						 <input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="host_name"  placeholder="Host name."> 
					</div>
					<div class="form-group">
						<input type="text"
							class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="port"  placeholder="Port.">
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="username"  placeholder="Username."> 
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="password"  placeholder="Password."> 
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="remote_path"  placeholder="Remote path."> 
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="local_path"  placeholder="Local path."> 
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="file_name"  placeholder="File name."> 
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="name_table_staging"  placeholder="Name table staging."> 
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="column_table_staging"  placeholder="Column table staging.">
						
					</div>
					<div class="form-group">
						<input type="text"
							class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="field"  placeholder="Field.">
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="field_insert"  placeholder="Field insert."> 
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="created"  placeholder="Sql create table."> 
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="name_table_warehouse"  placeholder="Name table warehouse.">
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="sql_insert"  placeholder="Sql insert table.">
					</div>
					<div class="form-group">
						<input
							type="text" class="form-control" id="exampleInputEmail1"
							aria-describedby="emailHelp" name="convert"  placeholder="Convert.">
					</div>
					
					<button type="submit" class="form-control btn btn-success">Submit</button>
				</form>
			</div>
		</div>
		<c:forEach items="${list}" var="tour">
			<datalist id="dsTourName">
				<option value="${tour.id.toLowerCase().trim() }"></option>
		</c:forEach>
		</datalist>
	</div>

	<script src="https://code.jquery.com/jquery-3.5.1.slim.min.js"
		integrity="sha384-DfXdz2htPH0lsSSs5nCTpuj/zy4C+OGpamoFVy38MVBnE+IbbVYUew+OrCXaRkfj"
		crossorigin="anonymous"></script>
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
