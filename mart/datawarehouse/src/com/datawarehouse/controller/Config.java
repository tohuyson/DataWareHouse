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




@WebServlet(urlPatterns = "/Config")
public class Config extends HttpServlet {
	private static final long serialVersionUID = 1L;
	Management log;
    public Config() throws SQLException {
        log = new Management();
        
    }
    
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		request.getRequestDispatcher("/WEB-INF/views/config.jsp").forward(request, response);				
			
	}

	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}


	

}
