#include <bits/stdc++.h>
using namespace std;
#include <ext/pb_ds/assoc_container.hpp>
#include <ext/pb_ds/tree_policy.hpp>
using namespace __gnu_pbds;
#define ll long long
#define ordered_set tree<int, null_type, less<int>, rb_tree_tag, tree_order_statistics_node_update>
#define sdefine \
    string s;   \
    cin >> s;
#define vll vector<ll>
#define pll pair<ll, ll>
#define vvll vector<vll>
#define vpll vector<pll>
#define mll map<ll, ll>
#define pb push_back
#define ppb pop_back
#define ff first
#define ss second
#define set_bits(a) __builtin_popcountll(a)
#define sz(x) ((int)(x).size())
#define all(x) (x).begin(), (x).end()
#define forl(i, a, b) for (int i = a; i < b; i++)
#define yesno(x, y)            \
    if ((x) == (y))            \
        cout << "YES" << endl; \
    else                       \
        cout << "NO" << endl;
#define endl "\n"
#define display(a, x, y)            \
    {                               \
        for (int i = x; i < y; i++) \
            cout << a[i] << " ";    \
        cout << endl;               \
    }
#define int long long

vector<string> all_tables = {"Employee", "Department", "Students", "Courses", "Enrollment"};

struct relation
{
    string table_name;
    vector<string> all_attr;
    map<string, int> attr_pos;
    vector<vector<string>> tuples;
};

int find_data_type(string);
int int_type(string);
vector<string> string_split_func(string, char);
pair<relation, bool> select_func(string, relation);
pair<relation, bool> project_func(string, relation);
pair<relation, bool> cartesian_product_func(relation, relation);
pair<relation, bool> query_evaluation(string);
pair<relation, bool> natural_join_func(relation, relation);
pair<relation, bool> union_func(relation, relation);
pair<relation, bool> intersect_func(relation, relation);
pair<relation, bool> difference_func(relation, relation);
pair<relation, bool> division_func(relation, relation);
pair<relation, bool> rename_func(relation, string);
pair<relation, bool> set_op_constraints(relation, relation);

class Table
{
    relation data;
    int tot_att, tot_tup;

public:
    Table(string t_name)
    {
        data.table_name = t_name;
        tot_att = 0;
        tot_tup = 0;
        read_data(t_name + ".csv");
    }

    void read_data(string file_name)
    {
        ifstream file;
        file.open("C:/Users/skwas/vs code folder/DBMS/" + file_name);
        if (!file.good())
        {
            cout << "FILE NOT FOUND" << endl;
            return;
        }
        string line;
        getline(file, line);
        data.all_attr = string_split_func(line, ',');
        while (getline(file, line))
        {
            vector<string> curr_row = string_split_func(line, ',');
            data.tuples.pb(curr_row);
            tot_tup++;
        }
        forl(i, 0, sz(data.all_attr))
        {
            data.attr_pos[data.all_attr[i]] = tot_att++;
        }
        file.close();
        return;
    }

    void print_data()
    {
        cout << data.table_name << endl;
        for (auto it : data.all_attr)
        {
            cout << it << " ";
        }
        cout << endl;
        for (auto v : data.tuples)
        {
            for (auto s : v)
            {
                cout << s << " ";
            }
            cout << endl;
        }
        return;
    }

    pair<relation, bool> query_traverse(string q)
    {
        bool ck = true;
        pair<relation, bool> output;
        output.ff = data;
        output.ss = true;
        stack<pair<char, string>> sub_query;
        stack<char> stk;
        for (int i = 0; i < sz(q); i++)
        {
            i = q.find('[', i);
            int pos = i + 1;
            if (i < sz(q))
            {
                stk.push('[');
            }
            else
                break;
            while (stk.size() > 0)
            {
                i++;
                if (q[i] == '[')
                {
                    stk.push(q[i]);
                }
                if (q[i] == ']')
                {
                    stk.pop();
                }
            }
            sub_query.push({q[pos - 2], q.substr(pos, (i - pos))});
        }
        stack<pair<char, string>> proc;
        while (!sub_query.empty())
        {
            pair<char, string> x = sub_query.top();
            sub_query.pop();
            if (x.ff == 'S')
            {
                // cout << "SELECT FUNCTION CALLED " << endl;
                output = select_func(x.ss, output.ff);
            }
            else if (x.ff == 'P')
            {
                // cout << "PROJECT FUNCTION CALLED " << endl;
                output = project_func(x.ss, output.ff);
            }
            else if (x.ff == 'C')
            {
                // cout << "SUB QUERY EVALUATION " << endl;
                pair<relation, bool> sub = query_evaluation(x.ss);
                output.ss &= (sub.ss);
                if (!output.ss)
                {
                    break;
                }
                // cout << "CARTESIAN PRODUCT FUNCTION CALLED" << endl;
                output = cartesian_product_func(sub.ff, output.ff);
            }
            else if (x.ff == 'J')
            {
                // cout << "SUB QUERY EVALUATION " << endl;
                pair<relation, bool> sub = query_evaluation(x.ss);
                output.ss &= (sub.ss);
                if (!output.ss)
                {
                    break;
                }
                // cout << "NATURAL-JOIN FUNCTION CALLED" << endl;
                output = natural_join_func(sub.ff, output.ff);
            }
            else if (x.ff == 'U')
            {
                // cout << "SUB QUERY EVALUATION " << endl;
                pair<relation, bool> sub = query_evaluation(x.ss);
                output.ss &= (sub.ss);
                if (!output.ss)
                {
                    break;
                }
                // cout << "UNION FUNCTION CALLED" << endl;
                output = union_func(sub.ff, output.ff);
            }
            else if (x.ff == 'I')
            {
                // cout << "SUB QUERY EVALUATION " << endl;
                pair<relation, bool> sub = query_evaluation(x.ss);
                output.ss &= (sub.ss);
                if (!output.ss)
                {
                    break;
                }
                // cout << "INTERSECT FUNCTION CALLED" << endl;
                output = intersect_func(sub.ff, output.ff);
            }
            else if (x.ff == 'D')
            {
                // cout << "SUB QUERY EVALUATION " << endl;
                pair<relation, bool> sub = query_evaluation(x.ss);
                output.ss &= (sub.ss);
                if (!output.ss)
                {
                    break;
                }
                // cout << "DIFFERENCE FUNCTION CALLED" << endl;
                output = difference_func(sub.ff, output.ff);
            }
            else if (x.ff == 'd')
            {
                // cout << "SUB QUERY EVALUATION " << endl;
                pair<relation, bool> sub = query_evaluation(x.ss);
                output.ss &= (sub.ss);
                if (!output.ss)
                {
                    break;
                }
                // cout << "DIVSION FUNCTION CALLED" << endl;
                output = division_func(sub.ff, output.ff);
            }
            else if (x.ff == 'R')
            {
                output = rename_func(output.ff, x.ss);
            }
            else
            {
                cout << "Query Syntax error " << endl;
            }
        }
        return output;
    }
};

vector<string> string_split_func(string st, char ch)
{
    vector<string> res;
    int pos1 = 0, ft, lt;
    int pos2 = st.find(ch, pos1);
    string tp;
    while (pos1 < sz(st))
    {
        if (pos2 < sz(st))
            tp = st.substr(pos1, (pos2 - pos1));
        else
            tp = st.substr(pos1);
        pos1 = pos2 + 1;
        pos2 = st.find(ch, pos1);
        ft = 0, lt = sz(tp) - 1;
        for (int i = 0; i < sz(tp); i++)
        {
            if (tp[i] == ' ')
            {
                ft++;
            }
            else
                break;
        }
        for (int i = sz(tp) - 1; i >= 0; i--)
        {
            if (tp[i] == ' ')
                lt--;
            else
                break;
        }
        tp = tp.substr(ft, (lt - ft) + 1);
        res.pb(tp);
    }
    return res;
}

bool check_bracket_validity(string(&q))
{
    // cout << q << endl;
    stack<int> st;
    bool ck = true;
    forl(i, 0, sz(q))
    {
        if ((q[i] == '[') || (q[i] == '('))
            st.push(q[i]);
        else if ((q[i] == ']') || (q[i] == ')'))
        {
            if (sz(st) != 0)
            {
                auto it = st.top();
                st.pop();
                if (q[i] == ']')
                {
                    if (it != '[')
                        return false;
                }
                else
                {
                    if (it != '(')
                        return false;
                }
            }
            else
            {
                return false;
            }
        }
        else
            continue;
    }
    if (sz(st) != 0)
        ck = false;
    if (!ck)
        cout << "INVALID BRACKET SEQUENCE" << endl;
    return ck;
}

string check_table_name(string(&q))
{
    int pos1 = -1, pos2 = -1;
    for (int i = sz(q) - 1; i >= 0; i--)
    {
        if (q[i] == ')')
            pos2 = i - 1;
        if (q[i] == '(')
            pos1 = i + 1;
        if ((pos1 != -1) && (pos2 != -1))
            break;
    }
    if ((pos1 == -1) || (pos2 == -1))
    {
        cout << "INVALID RELATION NAME" << endl;
        return "";
    }
    string nm = q.substr(pos1, (pos2 - pos1) + 1);
    forl(i, 0, sz(all_tables))
    {
        if (all_tables[i] == nm)
            return nm;
    }
    cout << "INVALID RELATION NAME" << endl;
    return "";
}

vector<string> convert_to_postfix(string(&st))
{
    stack<string> stk;
    vector<string> res;
    forl(i, 0, sz(st))
    {
        if (st[i] == ' ')
            continue;
        else if (st[i] == '^' || st[i] == '|')
        {
            while (!stk.empty() && stk.top() != "(")
            {
                auto iter = stk.top();
                stk.pop();
                res.pb(iter);
            }
            if (st[i] == '|')
                stk.push("|");
            else
                stk.push("^");
        }
        else if (st[i] == '(')
            stk.push("(");
        else if (st[i] == ')')
        {
            while (!stk.empty() && stk.top() != "(")
            {
                res.pb(stk.top());
                stk.pop();
            }
            stk.pop();
        }
        else
        {
            int j = i;
            while ((i < sz(st)) && (st[i] != '|') && (st[i] != '^') && (st[i] != ')') && (st[i] != '('))
                i++;
            res.pb(st.substr(j, i - j));
            i--;
        }
    }
    while (!stk.empty())
    {
        res.pb(stk.top());
        stk.pop();
    }
    return res;
}

vector<string> all_cond = {"<=", ">=", "!=", ">", "<", "="};

relation duplicate_erase(relation curr)
{
    relation res;
    res.table_name = curr.table_name;
    res.all_attr = curr.all_attr;
    res.attr_pos = curr.attr_pos;
    vll mark_pos(sz(curr.tuples), 0);
    forl(i, 0, sz(curr.tuples))
    {
        auto it = find(curr.tuples.begin() + i + 1, curr.tuples.end(), curr.tuples[i]);
        if (it == curr.tuples.end())
        {
            continue;
        }
        else
        {
            mark_pos[i] = 1;
        }
    }
    forl(i, 0, sz(curr.tuples))
    {
        if (mark_pos[i] == 1)
            continue;
        res.tuples.pb(curr.tuples[i]);
    }
    return res;
}

pair<relation, bool> set_op_constraints(relation tp1, relation tp2)
{
    relation output;
    bool ck = true;
    if (sz(tp1.all_attr) == sz(tp2.all_attr))
    {
        forl(i, 0, sz(tp1.all_attr))
        {
            if (find_data_type(tp1.tuples[0][i]) != find_data_type(tp2.tuples[0][i]))
            {
                ck = false;
                break;
            }
        }
    }
    else
    {
        ck = false;
    }
    if (!ck)
    {
        cout << "SET OPERATION CONSTRAINTS VIOLATED" << endl;
        return {output, ck};
    }
    forl(i, 0, sz(tp1.all_attr))
    {
        output.all_attr.pb(tp2.all_attr[i]);
    }
    forl(i, 0, sz(tp1.all_attr))
    {
        output.attr_pos[tp2.all_attr[i]] = i;
    }
    return {output, ck};
}

pair<relation, bool> rename_func(relation tp, string q)
{
    pair<relation, bool> res;
    res.ss = true;
    int lt = -1, rt = -1;
    for (int i = sz(q) - 1; i >= 0; i--)
    {
        if (q[i] == ')')
            rt = i;
        if (q[i] == '(')
            lt = i;
        if ((lt != -1) && (rt != -1))
            break;
    }
    vector<string> v;
    string nm = "";
    if (lt == -1)
    {
        v = string_split_func(q, ',');
    }
    else
    {
        if (lt != 0)
            v = string_split_func(q.substr(0, lt), ',');
        nm = q.substr(lt + 1, (rt - lt) - 1);
    }
    if ((sz(v) != 0) && (sz(v) != sz(tp.all_attr)))
    {
        cout << "INVALID COUNT OF ATTRIBUTES" << endl;
        res.ss = false;
        return res;
    }
    if (nm != "")
    {
        res.ff.table_name = nm;
    }
    forl(i, 0, sz(tp.all_attr))
    {
        if (i < sz(v))
        {
            res.ff.all_attr.pb(v[i]);
            res.ff.attr_pos[v[i]] = i;
        }
        else
        {
            res.ff.all_attr.pb(tp.all_attr[i]);
            res.ff.attr_pos[tp.all_attr[i]] = i;
        }
    }
    res.ff.tuples = tp.tuples;
    return res;
}

pair<relation, bool> division_func(relation tp1, relation tp2)
{
    pair<relation, bool> output;
    vector<string> req_attrs;
    forl(i, 0, sz(tp1.all_attr))
    {
        auto iter = find(all(tp2.all_attr), tp1.all_attr[i]);
        if (iter != tp2.all_attr.end())
            continue;
        else
        {
            req_attrs.pb(tp1.all_attr[i]);
        }
    }
    if (sz(req_attrs) == 0)
    {
        cout << "NOT VALID" << endl;
        output.ss = false;
        return output;
    }
    string st;
    forl(i, 0, sz(req_attrs))
    {
        if (i == sz(req_attrs) - 1)
        {
            st += (req_attrs[i]);
        }
        else
        {
            st += (req_attrs[i] + ",");
        }
    }
    pair<relation, bool> a, cp, df, b;
    a = project_func(st, tp1);
    if (a.ss)
        cp = cartesian_product_func(a.ff, tp2);
    if (cp.ss)
        df = difference_func(cp.ff, tp1);
    if (df.ss)
        b = project_func(st, df.ff);
    output = difference_func(a.ff, b.ff);
    return output;
}

pair<relation, bool> union_func(relation tp1, relation tp2)
{
    pair<relation, bool> output = set_op_constraints(tp1, tp2);
    if (!output.ss)
        return output;
    forl(i, 0, sz(tp1.tuples))
    {
        output.ff.tuples.pb(tp1.tuples[i]);
    }
    forl(i, 0, sz(tp2.tuples))
    {
        output.ff.tuples.pb(tp2.tuples[i]);
    }
    output.ff = duplicate_erase(output.ff);
    return output;
}

pair<relation, bool> intersect_func(relation tp1, relation tp2)
{
    pair<relation, bool> output = set_op_constraints(tp1, tp2);
    if (!output.ss)
        return output;
    vll common;
    forl(i, 0, sz(tp1.tuples))
    {
        auto it = find(all(tp2.tuples), tp1.tuples[i]);
        if (it == tp2.tuples.end())
        {
            continue;
        }
        else
        {
            common.pb(i);
        }
    }
    forl(i, 0, sz(common))
    {
        output.ff.tuples.pb(tp1.tuples[i]);
    }
    return output;
}

pair<relation, bool> difference_func(relation tp1, relation tp2)
{
    pair<relation, bool> output = set_op_constraints(tp1, tp2);
    if (!output.ss)
        return output;
    vll common;
    forl(i, 0, sz(tp1.tuples))
    {
        auto it = find(all(tp2.tuples), tp1.tuples[i]);
        if (it != tp2.tuples.end())
        {
            continue;
        }
        else
        {
            common.pb(i);
        }
    }
    forl(i, 0, sz(common))
    {
        output.ff.tuples.pb(tp1.tuples[i]);
    }
    return output;
}

pair<relation, bool> natural_join_func(relation tp1, relation tp2)
{
    relation res;
    res.table_name = tp1.table_name + " JOIN " + tp2.table_name;
    mll mp1, mp2;
    int pos = -1;
    bool check = true;
    for (auto it1 : tp1.all_attr)
    {
        auto it2 = find(all(tp2.all_attr), it1);
        pos++;
        if (it2 == tp2.all_attr.end())
            continue;
        mp1[pos] = (int)(it2 - tp2.all_attr.begin());
        mp2[(int)(it2 - tp2.all_attr.begin())] = pos;
    }
    if (pos == -1)
    {
        check = false;
        cout << "NO COMMON ATTRIBUTE FOUND" << endl;
        return {res, check};
    }
    for (auto r1 : tp1.tuples)
    {
        vector<string> v = r1;
        for (auto r2 : tp2.tuples)
        {
            bool ck = true;
            for (auto idx : mp1)
            {
                if (r1[idx.ff] != r2[idx.ss])
                {
                    ck = false;
                    break;
                }
            }
            if (ck)
            {
                forl(i, 0, sz(r2))
                {
                    if (mp2.find(i) == mp2.end())
                    {
                        v.pb(r2[i]);
                    }
                }
                res.tuples.pb(v);
            }
        }
    }
    res.all_attr = tp1.all_attr;
    res.attr_pos = tp1.attr_pos;
    int ct = sz(tp1.all_attr);
    forl(i, 0, sz(tp2.all_attr))
    {
        if (mp2.find(i) == mp2.end())
        {
            res.all_attr.pb(tp2.all_attr[i]);
            res.attr_pos[tp2.all_attr[i]] = ct++;
        }
    }
    res = duplicate_erase(res);
    return {res, check};
}

pair<relation, bool> cartesian_product_func(relation tp1, relation tp2)
{
    // cout << "INSIDE FUNCTION " << endl;
    relation res;
    bool ck = true;
    vector<string> tp1_attrs, tp2_attrs;
    string k1 = tp1.table_name, k2 = tp2.table_name;
    // cout << k1 << " " << k2 << endl;
    for (auto it : tp1.all_attr)
    {
        tp1_attrs.pb(k1 + "." + it);
    }
    for (auto it : tp2.all_attr)
    {
        string st = k2 + "." + it;
        tp2_attrs.pb(st);
        auto iter = find(all(tp1_attrs), st);
        if (iter != tp1_attrs.end())
        {
            cout << "CARTESIAN PRODUCT CONSTRAINTS VIOLATED" << endl;
            ck = false;
            return {res, ck};
        }
    }
    int ct = 0;
    res.table_name = tp1.table_name + "X" + tp2.table_name;
    for (auto it : tp1_attrs)
    {
        res.attr_pos[it] = ct++;
        res.all_attr.pb(it);
    }
    for (auto it : tp2_attrs)
    {
        res.attr_pos[it] = ct++;
        res.all_attr.pb(it);
    }
    // for (auto it : res.all_attr)
    // {
    //     cout << it << " ";
    // }
    // cout << endl;
    for (auto row1 : tp1.tuples)
    {
        for (auto row2 : tp2.tuples)
        {
            res.tuples.pb(row1);
            for (auto col : row2)
            {
                res.tuples[sz(res.tuples) - 1].pb(col);
            }
        }
    }
    res = duplicate_erase(res);
    return {res, ck};
}

int find_data_type(string val)
{
    int pos = val.find("'");
    if (pos >= sz(val))
        return 3;
    forl(i, pos + 1, sz(val) - 1)
    {
        if ((val[i] == ' '))
            continue;
        if ((val[i] < '0') || (val[i] > '9'))
            return 2;
    }
    return 1;
}

int int_type(string st)
{
    int res = 0;
    forl(i, 0, sz(st))
    {
        res *= 10;
        res += (int)(st[i] - '0');
    }
    return res;
}

pair<relation, bool> apply_constraint(string cond, relation output)
{
    relation tp;
    int cond_idx = -1, pos = -1;
    forl(i, 0, sz(all_cond))
    {
        pos = cond.find(all_cond[i]);
        if (pos < sz(cond))
        {
            cond_idx = i;
            break;
        }
    }
    if (cond_idx == -1)
        return {tp, false};
    string col_name = cond.substr(0, pos);
    // cout << cond_idx << endl;
    // cout << "constraints " << endl;
    // for (auto it : output.all_attr)
    // {
    //     cout << output.attr_pos[it] << " " << it << endl;
    // }
    // cout << col_name << endl;
    if (output.attr_pos.find(col_name) == output.attr_pos.end())
    {
        cout << "INVALID ATTRIBUTE" << endl;
        return {tp, false};
    }
    string val;
    if (cond_idx <= 2)
        val = cond.substr(pos + 2);
    else
        val = cond.substr(pos + 1);
    int typ = find_data_type(val);
    string col_name2;
    int k2;
    int k = output.attr_pos[col_name];
    tp.all_attr = output.all_attr;
    tp.attr_pos = output.attr_pos;
    tp.table_name = output.table_name;
    if (typ == 3)
    {
        col_name2 = val;
        if (output.attr_pos.find(col_name2) == output.attr_pos.end())
        {
            cout << "INVALID ATTRIBUTE" << endl;
            return {tp, false};
        }
        k2 = output.attr_pos[col_name2];
        if (find_data_type(output.tuples[0][k2]) == 2)
        {
            typ = 4;
        }
    }
    else
    {
        int pos = val.find("'");
        val = val.substr(pos + 1, sz(val) - 2);
    }
    // cout << typ << endl;
    forl(i, 0, sz(output.tuples))
    {
        if (typ == 1)
        {
            if ((cond_idx == 0) && (int_type(output.tuples[i][k]) <= int_type(val)))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 1) && (int_type(output.tuples[i][k]) >= int_type(val)))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 2) && (int_type(output.tuples[i][k]) != int_type(val)))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 3) && (int_type(output.tuples[i][k]) > int_type(val)))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 4) && (int_type(output.tuples[i][k]) < int_type(val)))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 5) && (int_type(output.tuples[i][k]) == int_type(val)))
            {
                tp.tuples.pb(output.tuples[i]);
            }
        }
        else if (typ == 2)
        {
            if ((cond_idx == 0) && (output.tuples[i][k] <= val))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 1) && (output.tuples[i][k] >= val))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 2) && (output.tuples[i][k] != val))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 3) && (output.tuples[i][k] > val))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 4) && (output.tuples[i][k] < val))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 5) && (output.tuples[i][k] == val))
            {
                tp.tuples.pb(output.tuples[i]);
            }
        }
        else if (typ == 3)
        {
            if ((cond_idx == 0) && (int_type(output.tuples[i][k]) <= int_type(output.tuples[i][k2])))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 1) && (int_type(output.tuples[i][k]) >= int_type(output.tuples[i][k2])))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 2) && (int_type(output.tuples[i][k]) != int_type(output.tuples[i][k2])))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 3) && (int_type(output.tuples[i][k]) > int_type(output.tuples[i][k2])))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 4) && (int_type(output.tuples[i][k]) < int_type(output.tuples[i][k2])))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 5) && (int_type(output.tuples[i][k]) == int_type(output.tuples[i][k2])))
            {
                tp.tuples.pb(output.tuples[i]);
            }
        }
        else
        {
            if ((cond_idx == 0) && (output.tuples[i][k] <= output.tuples[i][k2]))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 1) && (output.tuples[i][k] >= output.tuples[i][k2]))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 2) && (output.tuples[i][k] != output.tuples[i][k2]))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 3) && (output.tuples[i][k] > output.tuples[i][k2]))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 4) && (output.tuples[i][k] < output.tuples[i][k2]))
            {
                tp.tuples.pb(output.tuples[i]);
            }
            if ((cond_idx == 5) && (output.tuples[i][k] == output.tuples[i][k2]))
            {
                tp.tuples.pb(output.tuples[i]);
            }
        }
    }
    return {tp, true};
}

pair<relation, bool> select_func(string args, relation output)
{
    vector<string> expr = convert_to_postfix(args);
    // cout << "POSTIX EXPRESSION: ";
    // for (auto it : expr)
    // {
    //     cout << it << " ";
    // }
    // cout << endl;
    stack<relation> stk;
    vector<relation> cond_res;
    pair<relation, bool> res;
    res.ss = true;
    forl(i, 0, sz(expr))
    {
        if ((expr[i] == "^") || (expr[i] == "|"))
        {
            res.ff.table_name = output.table_name;
            res.ff.all_attr = output.all_attr;
            res.ff.attr_pos = output.attr_pos;
            res.ff.tuples.clear();
            auto iter1 = stk.top();
            stk.pop();
            auto iter2 = stk.top();
            stk.pop();
            if (expr[i] == "^")
            {
                forl(j, 0, sz(iter1.tuples))
                {
                    forl(k, 0, sz(iter2.tuples))
                    {
                        if (iter1.tuples[j] == iter2.tuples[k])
                        {
                            res.ff.tuples.pb(iter1.tuples[j]);
                            break;
                        }
                    }
                }
            }
            else
            {
                forl(i, 0, sz(iter2.tuples))
                {
                    iter1.tuples.pb(iter2.tuples[i]);
                }
                res.ff = iter1;
            }
            stk.push(res.ff);
        }
        else
        {
            // cout << "else" << endl;
            auto tp = apply_constraint(expr[i], output);
            if (!tp.ss)
            {
                res.ss &= tp.ss;
                return res;
            }
            stk.push(tp.ff);
        }
    }
    if (sz(stk) != 0)
        res.ff = stk.top();
    else
        res.ff = output;
    res.ff = duplicate_erase(res.ff);
    return res;
}

pair<relation, bool> project_func(string q, relation output)
{
    vector<string> req_attrs = string_split_func(q, ',');
    relation res;
    bool ck = true;
    string invalid_attr;
    forl(i, 0, sz(req_attrs))
    {
        if (output.attr_pos.find(req_attrs[i]) == output.attr_pos.end())
        {
            invalid_attr = req_attrs[i];
            ck = false;
            break;
        }
    }
    // cout << "inside project " << ck << endl;
    if (ck)
    {
        res.table_name = output.table_name;
        forl(i, 0, sz(req_attrs))
        {
            // cout << req_attrs[i] << " ";
            res.all_attr.pb(req_attrs[i]);
            res.attr_pos[req_attrs[i]] = i;
        }
        // cout << endl;
        forl(i, 0, sz(output.tuples))
        {
            vector<string> curr_tuple;
            forl(j, 0, sz(req_attrs))
            {
                curr_tuple.pb(output.tuples[i][output.attr_pos[req_attrs[j]]]);
            }
            auto iter = find(res.tuples.begin(), res.tuples.end(), curr_tuple);
            if (iter == res.tuples.end())
            {
                res.tuples.pb(curr_tuple);
            }
        }
    }
    else
    {
        cout << invalid_attr << " is an invalid attribute" << endl;
    }
    if (ck)
    {
        res = duplicate_erase(res);
    }
    return {res, ck};
}

pair<relation, bool> query_evaluation(string q)
{
    pair<relation, bool> res;
    bool ck = check_bracket_validity(q);
    res.ss = ck;
    string nm = check_table_name(q);
    if ((!ck) || (nm == ""))
    {
        ck = false;
        return res;
    }
    // cout << ck << " query evaluation " << nm << endl;
    Table tp(nm);
    // tp.print_data();
    res = tp.query_traverse(q);
    return res;
}

/*
1. Select Operaion
    S[()](Students)
    S[(Did='1')|(Sid='1' ^ Fname='Michael')](Students)
    S[(Salary='6000')](Employee)
    S[(Fname=Lname)](Students)
2. Project Operation
    P[Sid,Fname,Lname](Students)
    (P[Cid](S[Did='1'](Courses)))
3. Rename Operation
    R[id,fn,ln(new_t)](P[Sid,Fname,Lname](S[Did='2'](Students)))
    R[(a)](P[Sid,Fname](Students))
4. Cartesian Product
    C[R[(a)](P[Sid, Fname](Students))](Students)
5. Division Operation
    d[(Enrollment)](P[Cid](S[Did='1'](Courses)))
6. Theta Join
    R[id,Fn(new_t)](P[a.Sid, a.Fname](S[(a.Sid=Students.Sid)^(a.Fname=Students.Fname)](C[R[(a)](P[Sid, Fname](Students))](Students))))
    P[Students.Sid,Students.Fname,Students.Lname,Enrollment.Cid](S[Enrollment.Sid=Students.Sid](C[(Enrollment)](Students)))
7. Union
    U[S[Did='1'](Students)](S[Fname='Michael'](Students))
8. Intersect
    I[S[Did='1'](Students)](S[Fname='Michael'](Students))
9. Difference
    D[S[Did='1'](Students)](S[Fname='Michael'](Students))
10. Natural Join
    J[(P[Eid,Fname](Employee))](P[Eid,Lname](Employee))
*/
int32_t main()
{
    while (true)
    {
        cout << "INPUT CHOICE: " << endl;
        int type;
        cin >> type;
        if (type == 0)
        {
            cout << "TERMINATED" << endl;
            return 0;
        }
        cin.ignore();
        string q;
        getline(cin, q);
        // cout << q << endl;
        pair<relation, bool> res = query_evaluation(q);
        // cout << res.ss << endl;
        if (res.ss)
        {
            cout << endl;
            cout << res.ff.table_name << endl;
            for (auto it : res.ff.all_attr)
            {
                cout << it << "\t";
            }
            cout << endl;
            for (auto row : res.ff.tuples)
            {
                for (auto it : row)
                {
                    cout << it << "\t";
                }
                cout << endl;
            }
        }
        else
        {
            cout << "INVALID QUERY" << endl;
        }
    }
}