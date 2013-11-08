<?php 
require_once 'config.php';
require_once 'php/Fakebook.php';

if (!isset($_POST) || $_POST['email'] == "" || $_POST['password'] == "") {
	header("location: error.php?code=2");;
}

$fakebook = new Fakebook();

if (!$fakebook->login($_POST['email'], $_POST['password'])) {
	header("location: error.php?code=3");;
}
else {
	header("location: home.php");
}