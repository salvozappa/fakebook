<?php 
require_once 'config.php';
require_once 'php/Fakebook.php';

if (!isset($_GET['fullname'])) {
	$fakebook->error(4);
}

$fullName = $_GET['fullname'];

$fakebook = new Fakebook();

$uid = $fakebook->getUserIdFromFullName($fullName);

if (!$uid) {
	$fakebook->error(8);
}
else {
	header("location: user.php?id=$uid");
}