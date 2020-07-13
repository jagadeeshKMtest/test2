/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javaapplication1;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author DELL
 */
public class JavaApplication1 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        List<String> str= new ArrayList<>();
        str.add("1");
        str.add("2");
        System.out.println(" trest"+ str.toString());
        System.out.println(str.toString().replace("[", "").replace("]", ""));
        
        
        // TODO code application logic here
    }
    
}
