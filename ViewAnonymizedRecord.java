package com;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
import javax.swing.JScrollPane;
import javax.swing.JFrame;
import java.awt.Font;
import javax.swing.table.TableColumn;
public class ViewAnonymizedRecord extends JFrame{
	DefaultTableModel dtm;
	JScrollPane jsp;
	JTable table;
	String file;
	String columns[]={"Zip Code","Education","Age","Gender","Occupation","Race","Relationship","Status","Country","Work Class"};
public ViewAnonymizedRecord(String f){
	file = f;
	setTitle("View Anonymized Record");
	dtm = new DefaultTableModel(){
		public boolean isCellEditable(){
			return false;
		}
	};
	table = new JTable(dtm);
	table.getTableHeader().setFont(new Font("Courier New",Font.BOLD,15));
	table.setFont(new Font("Courier New",Font.BOLD,14));
	table.setRowHeight(30);
	if(file.equals("adult.txt")){
		for(int i=0;i<columns.length;i++){
			dtm.addColumn(columns[i]);
		}
		for(int i=0;i<columns.length;i++){
			table.getColumnModel().getColumn(i).setPreferredWidth(150);
		}	
	}
	if(file.equals("dataset.txt")){
		dtm.addColumn("Zip Code");
		dtm.addColumn("Age");
		dtm.addColumn("Race");
		dtm.addColumn("Diagnosis");
		for(int i=0;i<4;i++){
			table.getColumnModel().getColumn(i).setPreferredWidth(150);
		}	
	}
	jsp = new JScrollPane(table);
	getContentPane().add(jsp);
}
}