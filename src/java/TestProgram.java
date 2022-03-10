import simpledb.transaction.Transaction;

/**
 * @author panrixin <panrixin@kuaishou.com>
 * Created on 2022-02-18
 */
public class TestProgram {
    static class Tree{
        int val;
        Tree left = null;
        Tree right = null;

        public Tree(int val) {
            this.val = val;
        }
    }
    public static void main(String[] args) {
        Tree tree1 = new Tree(1);
        Tree tree2 = new Tree(2);
        Tree tree3 = new Tree(3);
        Tree tree4 = new Tree(4);
        Tree tree5 = new Tree(5);
        Tree tree6 = new Tree(6);

        tree1.left = tree2;tree1.right = tree3;
        tree2.left = null;tree2.right = tree4;
        tree3.left = tree5;tree3.right = tree6;

        System.out.println(compute(tree1, 0,0));

        System.out.println(Math.pow(10,0));

    }
    double sum = 0;

    public static double compute(Tree root, int level, int res) {

        if (root == null){
            return 0;
        }
        int sum = root.val * (int)Math.pow(10, level) + res;
        if (root.left == null && root.right == null) {
            return sum;
        }
        return compute(root.left, level + 1, sum) + compute(root.right, level+1,sum);

    }
}
