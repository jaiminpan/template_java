package com.aiwhale.ferrari.report.control;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import template.dao.TestDao;
import template.model.test;

@RestController
@RequestMapping("/test")
@Slf4j
public class TestCtrl {

	@Autowired
	TestDao testDao;

	@GetMapping("/getById")
	public Boolean getChan(@RequestParam String id) {
		return testDao.getById(id);
	}
}
