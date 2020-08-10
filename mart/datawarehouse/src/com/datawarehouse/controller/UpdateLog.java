package com.datawarehouse.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.datawarehouse.dao.Management;
import com.datawarehouse.model.Log;
import com.google.gson.Gson;




@WebServlet(urlPatterns = "/Update")
public class UpdateLog extends HttpServlet {
	private static final long serialVersionUID = 1L;
	Management log;
    public UpdateLog() throws SQLException {
        log = new Management();
        
    }
    
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	}

	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter out = response.getWriter();
		
		
		Gson gson = new Gson();	
			try {
				out.print(gson.toJson(log.getLogs()));
				out.flush();
				out.close();		
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	
	private void write(HttpServletResponse response, Map<String,Object> map) throws IOException {
		response.setContentType("application/json");
		response.setCharacterEncoding("utf-8");
		response.getWriter().write(new Gson().toJson(map));
		
	}

}
