package com.datawarehouse.controller;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.datawarehouse.dao.Management;
import com.datawarehouse.model.Config;
import com.datawarehouse.model.Log;

@WebServlet("/Control")
public class Control extends HttpServlet {
	private static final long serialVersionUID = 1L;
	Management mn;

	public Control() throws SQLException {
		mn = new Management();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		try {
			String temp = request.getParameter("id_config");
			int id_config = Integer.parseInt(temp);
			ArrayList<Log> list = new Management().filterList(id_config);
			request.setAttribute("list", list);
			request.getRequestDispatcher("/WEB-INF/views/result.jsp").forward(request, response);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		doGet(request, response);
	}

}
