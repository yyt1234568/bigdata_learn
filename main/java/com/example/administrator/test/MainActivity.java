package com.example.administrator.test;

import android.app.AlertDialog;
import android.app.ProgressDialog;
import android.content.DialogInterface;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

public class MainActivity extends AppCompatActivity implements View.OnClickListener{
    private Button btn_alert,btn_progress,btn_singleSel,btn_multiBtn, btn_cust;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        initView();
    }

    private void initView() {
        btn_alert = findViewById(R.id.btn_alert);
        btn_progress = findViewById(R.id.btn_progress);
        btn_singleSel = findViewById(R.id.btn_single_select);
        btn_multiBtn = findViewById(R.id.btn_multi_select);
        btn_cust = findViewById(R.id.btn_cust);

        btn_alert.setOnClickListener(this);
        btn_progress.setOnClickListener(this);
        btn_singleSel.setOnClickListener(this);
        btn_multiBtn.setOnClickListener(this);
        btn_cust.setOnClickListener(this);
    }

    @Override
    public void onClick(View view) {
        int id = view.getId();
        switch(id){
            case R.id.btn_alert:
                alert();
                break;
            case R.id.btn_progress:
                progress();
                break;
            case R.id.btn_single_select:
                singleSel();
                break;
            case R.id.btn_multi_select:
                multiSel();
                break;
            case R.id.btn_cust:
                customDlg();
                break;
        }
    }

    private void alert(){
        AlertDialog.Builder dialog = new AlertDialog.Builder(MainActivity.this);
        dialog.setTitle("title");//对话框最上面的字
        dialog.setMessage("content");//对话框中部的字
        dialog.setCancelable(false);//如果是false，点击其他位置或者返回键无效，这个地方默认为true


        dialog.setPositiveButton("右边", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                Toast.makeText(MainActivity.this, "右边", Toast.LENGTH_SHORT).show();
            }

        });

        //需要第二个按钮才添加如下的setNegativeButton()
        dialog.setNegativeButton("左边", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {//// 第一个参数 dialog int which 那个条目
                Toast.makeText(MainActivity.this, "左边", Toast.LENGTH_SHORT).show();
            }

        });



        dialog.show();//以下为对话框最下面的选择项

    }


    private void progress(){

        ProgressDialog progressDialog = new ProgressDialog
                (MainActivity.this);
        progressDialog.setTitle("对话框上部的字");
        progressDialog.setMessage("正在加载.....");
        progressDialog.setCancelable(true);//如果是false，点击其他位置或者返回键无效，默认为true
        progressDialog.show();

        //progressDialog.dismiss();//此句让progressDialog消失
    }

    private void singleSel(){
        AlertDialog.Builder builder = new AlertDialog.Builder(MainActivity.this);
        //builder.setIcon(R.drawable.ic_launcher);
        builder.setTitle("选择一个城市");
        //    指定下拉列表的显示数据
        final String[] cities = {"广州", "上海", "北京", "香港", "澳门"};
        //    设置一个下拉的列表选择项
        builder.setItems(cities, new DialogInterface.OnClickListener()
        {
            @Override
            public void onClick(DialogInterface dialog, int which)
            {
                Toast.makeText(MainActivity.this, "选择的城市为：" + cities[which], Toast.LENGTH_SHORT).show();
            }
        });
        builder.show();
    }

    private void multiSel(){
        AlertDialog.Builder builder = new AlertDialog.Builder(MainActivity.this);
        //builder.setIcon(R.drawable.ic_launcher);
        builder.setTitle("爱好");
        final String[] hobbies = {"篮球", "足球", "网球", "斯诺克"};
        //    设置一个单项选择下拉框
        /**
         * 第一个参数指定我们要显示的一组下拉多选框的数据集合
         * 第二个参数代表哪几个选项被选择，如果是null，则表示一个都不选择，如果希望指定哪一个多选选项框被选择，
         * 需要传递一个boolean[]数组进去，其长度要和第一个参数的长度相同，例如 {true, false, false, true};
         * 第三个参数给每一个多选项绑定一个监听器
         */
        builder.setMultiChoiceItems(hobbies, null, new DialogInterface.OnMultiChoiceClickListener()
        {
            StringBuffer sb = new StringBuffer(100);
            @Override
            public void onClick(DialogInterface dialog, int which, boolean isChecked)
            {
                if(isChecked)
                {
                    sb.append(hobbies[which] + ", ");
                }
                Toast.makeText(MainActivity.this, "爱好为：" + sb.toString(), Toast.LENGTH_SHORT).show();
            }
        });
        builder.setPositiveButton("确定", new DialogInterface.OnClickListener()
        {
            @Override
            public void onClick(DialogInterface dialog, int which)
            {

            }
        });
        builder.setNegativeButton("取消", new DialogInterface.OnClickListener()
        {
            @Override
            public void onClick(DialogInterface dialog, int which)
            {

            }
        });
        builder.show();
    }


    private void customDlg(){

        AlertDialog.Builder builder = new AlertDialog.Builder(MainActivity.this);
        //builder.setIcon(R.drawable.ic_launcher);
        builder.setTitle("请输入用户名和密码");
        //    通过LayoutInflater来加载一个xml的布局文件作为一个View对象
        View view = LayoutInflater.from(MainActivity.this).inflate(R.layout.dialog, null);
        //    设置我们自己定义的布局文件作为弹出框的Content
        builder.setView(view);

        final EditText username = (EditText)view.findViewById(R.id.username);
        final EditText password = (EditText)view.findViewById(R.id.password);

        builder.setPositiveButton("确定", new DialogInterface.OnClickListener()
        {
            @Override
            public void onClick(DialogInterface dialog, int which)
            {
                String a = username.getText().toString().trim();
                String b = password.getText().toString().trim();
                //    将输入的用户名和密码打印出来
                Toast.makeText(MainActivity.this, "用户名: " + a + ", 密码: " + b, Toast.LENGTH_SHORT).show();
            }
        });
        builder.setNegativeButton("取消", new DialogInterface.OnClickListener()
        {
            @Override
            public void onClick(DialogInterface dialog, int which)
            {

            }
        });
        builder.show();
    }
}
