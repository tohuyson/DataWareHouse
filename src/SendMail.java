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
	   public static boolean sendMail(String mail, String subject, String token ) {
	        Properties props = new Properties();
	        props.put("mail.smtp.auth", "true");
	        props.put("mail.smtp.starttls.enable", "true");
	        props.put("mail.smtp.host", "smtp.gmail.com");
	        props.put("mail.smtp.port", "587");
	        Session session = Session.getInstance(props, new Authenticator() {
	            @Override
	            protected javax.mail.PasswordAuthentication getPasswordAuthentication() {
	                return new PasswordAuthentication("17130047@st.hcmuaf.edu.vn", "giangkhanh1599");
	            }
	        });
	        try {
	            Message message = new MimeMessage(session);
	            message.setHeader("Content-Type", "text/plain; charset=UTF-8");
	            message.setFrom(new InternetAddress("17130047@st.hcmuaf.edu.vn"));
	            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(mail));
	            message.setSubject(subject);
	            message.setText(token);
	            Transport.send(message);
	        } catch (MessagingException e) {
	            return false;
	        }
	        return true;
	    }

	    public static void main(String[] args) {
	        
	        System.out.println(sendMail("giang1599.ng@gmail.com","hihihi","lay lai mat khau"));
	    }
}
