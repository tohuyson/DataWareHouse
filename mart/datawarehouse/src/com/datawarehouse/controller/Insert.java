package com.datawarehouse.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.datawarehouse.dao.Management;
import com.datawarehouse.model.Log;
import com.google.gson.Gson;

@WebServlet(urlPatterns = "/Insert")
public class Insert extends HttpServlet {
	private static final long serialVersionUID = 1L;
	Management config;

	public Insert() throws SQLException {
		config = new Management();

	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		try {
			String host = request.getParameter("host_name");
			String temp_port = request.getParameter("port");
			int port = Integer.parseInt(temp_port);
			String username = request.getParameter("username");
			String password = request.getParameter("password");
			String remote = request.getParameter("remote_path");
			String local = request.getParameter("local_path");
			String file_name = request.getParameter("file_name");
			String name_table = request.getParameter("name_table_staging");
			String temp_column = request.getParameter("column_table_staging");
			int column = Integer.parseInt(temp_column);
			String field = request.getParameter("field");
			String field_insert = request.getParameter("field_insert");
			String created = request.getParameter("created");
			String name_table_warehouse = request.getParameter("name_table_warehouse");
			String sql_insert = request.getParameter("sql_insert");
			String field_convert = request.getParameter("convert");
			config.insert(host, port, username, password, remote, local, file_name, name_table, column, field, field_insert, created, name_table_warehouse, sql_insert, field_convert);
		}catch(Exception e) {
			e.printStackTrace();
		}
		request.getRequestDispatcher("/WEB-INF/views/success.jsp").forward(request, response);

	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}

}
