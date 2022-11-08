package fis;

import java.util.*;

public class Combination {
    private Integer n, k;
    private String[] elements;
    private List<String> ans;

    public Combination(List<String> items, int k) {
        initrun(items, k);
    }

    public void initrun(List<String> items, int k) {
        this.k = k;
        this.n = items.size();
        this.elements = new String[n];
        items.toArray(elements);
        this.ans = new ArrayList<String>();
    }

    public void execute() {
        int idx = 0, cnt = 0;
        String now = "";
        backtrack(now, idx, cnt);
    }

    public void backtrack(String now, int idx, int cnt) {
        if (cnt == k) {
            String s = now.substring(0, now.length() - 2);
            ans.add(s);
            return;
        }
        if (idx >= n) {
            return;
        }
        String s = now;
        backtrack(s, idx + 1, cnt);
        String next = s + elements[idx] + ", ";
        backtrack(next, idx + 1, cnt + 1);
        return;
    }

    public List<String> getAns() {
        return this.ans;
    }
}
