<?php 
require_once 'config.php';
require_once 'php/Fakebook.php';

$fakebook = new Fakebook();

if (!isset($_POST['message'])) {
	$fakebook->error(4);	// Missing parameters
}

$message = $_POST['message'];

$fakebook->pushStatus($message);

$fakebook->ok(2);