package main;

import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class SendMail {
	public SendMail() {
	}

	public static boolean sendMail(String mail, String subject, String token) {
		Properties props = new Properties();
		
		// xác thực người dùng bằng lệnh AUTH
		props.put("mail.smtp.auth", "true");
		//cho phép sử dụng starttls(nếu có hỗ trợ) chuyển kết nối sang kết nối bảo vệ bằng TSL trước khi đăng nhập
		props.put("mail.smtp.starttls.enable", "true");
		// máy chủ smtp để kết nối
		props.put("mail.smtp.host", "smtp.gmail.com");
		// cong may chu để kết nối
		props.put("mail.smtp.port", "587");
		Session session = Session.getInstance(props, new Authenticator() {
			@Override
			// xác thực gmail và mật khẩu
			protected javax.mail.PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication("17130047@st.hcmuaf.edu.vn", "giangkhanh1599");
			}
		});
		try {
			// khoi tạo tin nhắn email kiểu Mime
			Message message = new MimeMessage(session);
			message.setHeader("Content-Type", "text/plain; charset=UTF-8");
			message.setFrom(new InternetAddress("17130047@st.hcmuaf.edu.vn"));
			// chỉ định người nhận
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(mail));
			// đặt tiêu đề
			message.setSubject(subject);
			// đặt nội dung
			message.setText(token);
			// tiến hành gửi 
			Transport.send(message);
		} catch (MessagingException e) {
			return false;
		}
		return true;
	}

	public static void main(String[] args) {

//	        System.out.println(sendMail("phantrancongthanh240499@gmail.com","hihihi","lay lai mat khau"));
	}
}
