package com.atguigu.gmall.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
//@RestController等同于@Controller + @ResponseBody
public class FirstController {
    @RequestMapping("/first")
//    @ResponseBody
    public String first(@RequestParam("haha")String username,@RequestParam("heihei")String password){
        System.out.println(username + ":::" + password);
        return "success";
    }
}
