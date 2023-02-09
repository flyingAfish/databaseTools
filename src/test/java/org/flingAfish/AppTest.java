package org.flingAfish;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        String str = "MAP < primitive_type, data_type >";
        String str1 = " DECIMAL(precision, scale)";

        System.out.println(str.split("<")[0].split("\\(")[0].trim());
        System.out.println(str1.split("<")[0].split("\\(")[0].trim());

        System.out.println(str);
        System.out.println(null + str);
    }
}
