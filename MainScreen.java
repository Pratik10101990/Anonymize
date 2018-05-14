package com;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.Font;
import javax.swing.JPanel;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JLabel;
import java.awt.Dimension;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JOptionPane;
import javax.swing.JFrame;
import javax.swing.UIManager;
import java.awt.Cursor;
import javax.swing.JFileChooser;
import java.io.File;
import java.util.ArrayList;
import java.io.FileWriter;
import org.jfree.ui.RefineryUtilities;
import java.util.Random;
import javax.swing.table.DefaultTableModel;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
public class MainScreen extends JFrame{
	String name;
	JPanel p1,p2,p3,p4;
	JLabel l1;
	Font f1,f2;
	JButton b1,b2,b3,b4,b5;
	JScrollPane jsp;
	JFileChooser chooser;
	File file;
	JTable table;
	MyTableModel dtm;
	String columns[]={"Age","Work Class","Zip Code","Education","Education-Num","Marital Status","Occupation","Relationship","Race","Sex","Balance","Unknown","Hours Per Week","Country","Salary"};
	static double time;
	static int records;
public MainScreen(){
	super("TDS Algorithm");
	setLayout(new BorderLayout());
	f1 = new Font("Courier New",Font.BOLD,16);
	f2 = new Font("Courier New",Font.BOLD,13);
	p1 = new JPanel();
	p1.setLayout(new BorderLayout());
	p1.setBackground(Color.white);
	
	chooser = new JFileChooser(new File("dataset"));

	p2 = new JPanel();
	l1 = new JLabel("<HTML><BODY><CENTER>A Top-Down k-Anonymization Implementation for Apache Spark</CENTER></BODY></HTML>");
	l1.setFont(f1);
	p2.add(l1);
	p2.setBackground(Color.white);

	p3 = new JPanel();
	p3.setPreferredSize(new Dimension(200,80));
	p3.setBackground(Color.white);
	
	b1 = new JButton("Upload Dataset");
	b1.setFont(f2);
	p3.add(b1);
	b1.addActionListener(new ActionListener(){
		public void actionPerformed(ActionEvent ae){
			int option = chooser.showOpenDialog(MainScreen.this);
			if(option == JFileChooser.APPROVE_OPTION){
				file = chooser.getSelectedFile();
				Cursor hourglassCursor = new Cursor(Cursor.WAIT_CURSOR);
				setCursor(hourglassCursor);
				readDataset();
				records = dtm.getRowCount();
				Cursor normalCursor = new Cursor(Cursor.DEFAULT_CURSOR);
				setCursor(normalCursor);
			}
		}
	});

	b2 = new JButton("Run Distributed TDS Algorithm");
	b2.setFont(f2);
	p3.add(b2);
	b2.addActionListener(new ActionListener(){
		public void actionPerformed(ActionEvent ae){
			Cursor hourglassCursor = new Cursor(Cursor.WAIT_CURSOR);
			setCursor(hourglassCursor);
			long start = System.currentTimeMillis();
			SparkAnonymize.run(file);
			long end = System.currentTimeMillis();
			double s = (double)start;
			double e = (double)end;
			time = (e - s);
			Cursor normalCursor = new Cursor(Cursor.DEFAULT_CURSOR);
			setCursor(normalCursor);
			JOptionPane.showMessageDialog(MainScreen.this,"Anonymize Process Completed");
		}
	});

	b3 = new JButton("View Anonymized Records");
	b3.setFont(f2);
	p3.add(b3);
	b3.addActionListener(new ActionListener(){
		public void actionPerformed(ActionEvent ae){
			ViewAnonymizedRecord var = new ViewAnonymizedRecord(file.getName());
			if(file.getName().equals("dataset.txt")){
				for(Map.Entry<String,ArrayList<String>> me : SparkAnonymize.anonymize.entrySet()){
					String key = me.getKey();
					ArrayList<String> list = me.getValue();
					for(int i=0;i<list.size();i++){
						String arr[] = list.get(i).split(",");
						var.dtm.addRow(arr);
					}
				}
			}
			if(file.getName().equals("adult.txt")){
				for(Map.Entry<String,ArrayList<String>> me : SparkAnonymize.anonymize.entrySet()){
					String key = me.getKey();
					ArrayList<String> list = me.getValue();
					for(int i=0;i<list.size();i++){
						String arr[] = list.get(i).split(",");
						var.dtm.addRow(arr);
					}
				}
			}
			var.setVisible(true);
			var.setExtendedState(JFrame.MAXIMIZED_BOTH);
		}
	});

	b4 = new JButton("Run Time Graph");
	b4.setFont(f2);
	p3.add(b4);
	b4.addActionListener(new ActionListener(){
		public void actionPerformed(ActionEvent ae){
			Chart chart1 = new Chart("Run Time Graph");
			chart1.pack();
			RefineryUtilities.centerFrameOnScreen(chart1);
			chart1.setVisible(true);
		}
	});

	b5 = new JButton("Exit");
	b5.setFont(f2);
	p3.add(b5);
	b5.addActionListener(new ActionListener(){
		public void actionPerformed(ActionEvent ae){
			System.exit(0);
		}
	});

	
	p1.add(p2,BorderLayout.NORTH);
	p1.add(p3,BorderLayout.CENTER);
	
	p4 = new JPanel();
	p4.setLayout(new BorderLayout());
	p4.setBackground(Color.white);
	dtm = new MyTableModel(){
		public boolean isCellEditable(){
			return false;
		}
	};
	table = new JTable(dtm);
	table.getTableHeader().setFont(new Font("Courier New",Font.BOLD,15));
	table.setFont(new Font("Courier New",Font.BOLD,14));
	table.setRowHeight(30);
	table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
	jsp = new JScrollPane(table);
	p4.add(jsp,BorderLayout.CENTER);
		
	add(p1,BorderLayout.NORTH);
	add(p4,BorderLayout.CENTER);
}

public static void main(String a[])throws Exception{
	 UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
	 MainScreen ps = new MainScreen();
	 ps.setVisible(true);
	 ps.setExtendedState(JFrame.MAXIMIZED_BOTH);
}
public void clearTable(){
	for(int i=table.getRowCount()-1;i>=0;i--){
		dtm.removeRow(i);
	}
	for(int i=table.getColumnCount()-1;i>=0;i--){
		dtm.removeColumn(i);
	}
}
public void readDataset(){
	try{
		clearTable();
		if(file.getName().equals("adult.txt")){
			for(int i=0;i<columns.length;i++){
				dtm.addColumn(columns[i]);
			}
			for(int i=0;i<columns.length;i++){
				table.getColumnModel().getColumn(i).setPreferredWidth(150);
			}	
		}
		if(file.getName().equals("dataset.txt")){
			dtm.addColumn("Zip Code");
			dtm.addColumn("Age");
			dtm.addColumn("Race");
			dtm.addColumn("Diagnosis");
			for(int i=0;i<4;i++){
				table.getColumnModel().getColumn(i).setPreferredWidth(150);
			}	
		}
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = null;
		while((line = br.readLine()) != null){
			line = line.trim();
			if(line.length() > 0){
				String arr[] = line.split(",");
				dtm.addRow(arr);
			}
		}
		br.close();
	}catch(Exception e){
		e.printStackTrace();
	}
}
}
class MyTableModel extends DefaultTableModel {
    public void removeColumn(int column){
		columnIdentifiers.remove(column);
		for(Object row: dataVector){
			((java.util.Vector) row).remove(column);
		}
		fireTableStructureChanged();
    }	
}