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




@WebServlet(urlPatterns = "/New")
public class New extends HttpServlet {
	private static final long serialVersionUID = 1L;
	Management log;
    public New() throws SQLException {
        log = new Management();
        
    }
    
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
		
			String message = "New log !!!";			
				List<Log> list = log.getLogs();
				request.setAttribute("message", message);
				request.setAttribute("list", list);
				request.getRequestDispatcher("/WEB-INF/views/news.jsp").forward(request, response);						
						
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}


	

}
