<?php 
require_once 'config.php';
require_once 'php/Fakebook.php';

if (!isset($_POST['message'])) {
	$fakebook->error(4);	// Missing parameters
}

$message = $_POST['message'];

$fakebook = new Fakebook();

$fakebook->pushStatus($message);

$fakebook->ok(2);