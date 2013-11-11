<?php 
require_once 'config.php';
require_once 'php/Fakebook.php';

$fakebook = new Fakebook();

if (!isset($_GET['id'])) {
	$fakebook->error(4);
}

$id = $_GET['id'];

if (isset($_GET['unlike'])) {
	$fakebook->unlikeStatus($id);
} else {
	$fakebook->likeStatus($id);
}

header('Location: ' . $_SERVER['HTTP_REFERER']);