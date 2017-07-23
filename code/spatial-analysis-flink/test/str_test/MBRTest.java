package str_test;

import de.tu_berlin.dima.datatype.MBR;
import de.tu_berlin.dima.datatype.Point;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Suite;

/**
 * Created by JML on 6/27/17.
 */
@Suite.SuiteClasses(MBRTest.class)
public class MBRTest {
    private MBR mbr;

    private void prepareData(){
        Point p1 = TestUtil.create2DPoint(0,4);
        Point p2 = TestUtil.create2DPoint(3,0);
        this.mbr = new MBR(2);
        this.mbr.addPoint(p1);
        this.mbr.addPoint(p2);
    }

    private void resetData(){
        Point p1 = TestUtil.create2DPoint(0,0);
        Point p2 = TestUtil.create2DPoint(3,4);
        this.mbr.setMinPoint(p1);
        this.mbr.setMaxPoint(p2);
    }


    @Test
    public void testInitialization(){
        // Max point should be (3,4), min (0.0)
        Point p1 = TestUtil.create2DPoint(0,4);
        Point p2 = TestUtil.create2DPoint(3,0);

        MBR mbr = new MBR(2);
        Assert.assertEquals("Wrong initialized flag", true, mbr.isInitialized());
        mbr.addPoint(p1);
        mbr.addPoint(p2);

        Assert.assertEquals("Wrong max point", TestUtil.create2DPoint(3,4), mbr.getMaxPoint());
        Assert.assertEquals("Wrong min point", TestUtil.create2DPoint(0,0), mbr.getMinPoint());
        Assert.assertEquals("Wrong initialized flag", false, mbr.isInitialized());

        // Change point to see if the mbr change
        p1.setDimension(1,0);
        p2.setDimension(2,1);
        Assert.assertEquals("Wrong max point", TestUtil.create2DPoint(3,4), mbr.getMaxPoint());
        Assert.assertEquals("Wrong min point", TestUtil.create2DPoint(0,0), mbr.getMinPoint());
        Assert.assertEquals("Wrong initialized flag", false, mbr.isInitialized());

        // Reset p1, p2
        MBR mbr2 = new MBR(TestUtil.create2DPoint(0,0), TestUtil.create2DPoint(3,4));
        Assert.assertEquals("Wrong max point", TestUtil.create2DPoint(3,4), mbr2.getMaxPoint());
        Assert.assertEquals("Wrong min point", TestUtil.create2DPoint(0,0), mbr2.getMinPoint());
        Assert.assertEquals("Wrong initialized flag", false, mbr.isInitialized());

        // Reset p1, p2
        MBR mbr3 = new MBR(TestUtil.create2DPoint(3,4), TestUtil.create2DPoint(-1,5));
        Assert.assertEquals("Wrong max point", TestUtil.create2DPoint(3,5), mbr3.getMaxPoint());
        Assert.assertEquals("Wrong min point", TestUtil.create2DPoint(-1,4), mbr3.getMinPoint());
        Assert.assertEquals("Wrong initialized flag", false, mbr.isInitialized());
    }

    @Test
    public void testAddPoint(){
        prepareData();
        // Add point which is inside the MBR
        this.mbr.addPoint(TestUtil.create2DPoint(1,2));
        Assert.assertEquals("Wrong max point", TestUtil.create2DPoint(3,4), this.mbr.getMaxPoint());
        Assert.assertEquals("Wrong min point", TestUtil.create2DPoint(0,0), this.mbr.getMinPoint());

        // Add point which is on the corner of the MBR
        this.mbr.addPoint(TestUtil.create2DPoint(0,4));
        Assert.assertEquals("Wrong max point", TestUtil.create2DPoint(3,4), this.mbr.getMaxPoint());
        Assert.assertEquals("Wrong min point", TestUtil.create2DPoint(0,0), this.mbr.getMinPoint());
        // Add point which is outside of the MBR
        this.mbr.addPoint(TestUtil.create2DPoint(4,4));
        this.mbr.addPoint(TestUtil.create2DPoint(-1,0));
        Assert.assertEquals("Wrong max point", TestUtil.create2DPoint(4,4), this.mbr.getMaxPoint());
        Assert.assertEquals("Wrong min point", TestUtil.create2DPoint(-1,0), this.mbr.getMinPoint());
    }

    @Test
    public void testAddMBR(){
        prepareData();
        // Inside
        MBR mbr1 = new MBR(TestUtil.create2DPoint(1,1), TestUtil.create2DPoint(2,2));
        this.mbr.addMBR(mbr1);
        Assert.assertEquals("Wrong max point", TestUtil.create2DPoint(3,4), this.mbr.getMaxPoint());
        Assert.assertEquals("Wrong min point", TestUtil.create2DPoint(0,0), this.mbr.getMinPoint());

        // Outside
        MBR mbr2 = new MBR(TestUtil.create2DPoint(-1, -1), TestUtil.create2DPoint(4,4));
        this.mbr.addMBR(mbr2);
        Assert.assertEquals("Wrong max point", TestUtil.create2DPoint(4,4), this.mbr.getMaxPoint());
        Assert.assertEquals("Wrong min point", TestUtil.create2DPoint(-1,-1), this.mbr.getMinPoint());
        resetData();

        // Intersect
        MBR mbr3 = new MBR(TestUtil.create2DPoint(2, 2), TestUtil.create2DPoint(4,4));
        this.mbr.addMBR(mbr3);
        Assert.assertEquals("Wrong max point", TestUtil.create2DPoint(4,4), this.mbr.getMaxPoint());
        Assert.assertEquals("Wrong min point", TestUtil.create2DPoint(0,0), this.mbr.getMinPoint());
        resetData();
    }

    @Test
    public void testContains(){
        // Test contains Point
        prepareData();
        Point p1 = TestUtil.create2DPoint(2,2);
        Point p2 = TestUtil.create2DPoint(3,4);
        Point p3 = TestUtil.create2DPoint(4,4);
        Assert.assertEquals("Wrong contains(point)", true, this.mbr.contains(p1));
        Assert.assertEquals("Wrong contains(point)", true, this.mbr.contains(p2));
        Assert.assertEquals("Wrong contains(point)", false, this.mbr.contains(p3));

        // Test contains MBR
        // Inside
        MBR mbr1 = new MBR(TestUtil.create2DPoint(1,1), TestUtil.create2DPoint(2,2));
        Assert.assertEquals("Wrong contains(mbr)", true, this.mbr.contains(mbr1));

        MBR mbr11 = new MBR(TestUtil.create2DPoint(0,0), TestUtil.create2DPoint(3,4));
        Assert.assertEquals("Wrong contains(mbr)", true, this.mbr.contains(mbr11));

        // Outside
        MBR mbr2 = new MBR(TestUtil.create2DPoint(-1, -1), TestUtil.create2DPoint(4,4));
        Assert.assertEquals("Wrong contains(mbr)", false, this.mbr.contains(mbr2));

        // Intersect
        MBR mbr3 = new MBR(TestUtil.create2DPoint(2, 2), TestUtil.create2DPoint(4,4));
        Assert.assertEquals("Wrong contains(mbr)", false, this.mbr.contains(mbr3));


    }

//    @Test
//    public void testCompare(){
//        Assert.assertEquals(true, false);
//    }

    @Test
    public void testEquals(){
        prepareData();
        MBR mbr1 = new MBR(TestUtil.create2DPoint(0,0), TestUtil.create2DPoint(3,4));
        Assert.assertEquals(true, this.mbr.equals(mbr1));

        MBR mbr2 = new MBR(TestUtil.create2DPoint(1,1), TestUtil.create2DPoint(3,4));
        Assert.assertEquals(false, this.mbr.equals(mbr2));


    }

    @Test
    public void testIntersects(){
        prepareData();
        // Inside
        MBR mbr1 = new MBR(TestUtil.create2DPoint(1,1), TestUtil.create2DPoint(2,2));
        Assert.assertEquals("Wrong intersect", true, this.mbr.intersects(mbr1));

        MBR mbr11 = new MBR(TestUtil.create2DPoint(0,0), TestUtil.create2DPoint(3,4));
        Assert.assertEquals("Wrong intersect", true, this.mbr.intersects(mbr11));

        // Outside
        MBR mbr2 = new MBR(TestUtil.create2DPoint(-1, -1), TestUtil.create2DPoint(4,4));
        Assert.assertEquals("Wrong intersect", true, this.mbr.intersects(mbr2));

        // Intersect
        MBR mbr3 = new MBR(TestUtil.create2DPoint(2, 2), TestUtil.create2DPoint(4,4));
        Assert.assertEquals("Wrong intersect", true, this.mbr.intersects(mbr3));

        // Intersect
        MBR mbr4 = new MBR(TestUtil.create2DPoint(3, 4), TestUtil.create2DPoint(5,5));
        Assert.assertEquals("Wrong intersect", true, this.mbr.intersects(mbr4));

        // No intersect
        MBR mbr5 = new MBR(TestUtil.create2DPoint(4, 4), TestUtil.create2DPoint(5,5));
        Assert.assertEquals("Wrong intersect", false, this.mbr.intersects(mbr5));

        // Intersect with circle
        Point outsidePoint = TestUtil.create2DPoint(4,4);
        Point insidePoint = TestUtil.create2DPoint(2,2);
        Assert.assertEquals("Wrong intersect", true, this.mbr.intersects(outsidePoint, 1f));
        Assert.assertEquals("Wrong intersect", false, this.mbr.intersects(outsidePoint, 0.5f));
        Assert.assertEquals("Wrong intersect", true, this.mbr.intersects(insidePoint, 0.5f));


        // Test with wrong min,max input for MBR
        MBR mbr6 = new MBR(TestUtil.create2DPoint(3,9), TestUtil.create2DPoint(-1,5));
        Assert.assertEquals("Wrong contains (mbr) ", true, (mbr6.intersects(TestUtil.create2DPoint(1,2), 5f)));

    }
}
