<?php 
require_once 'config.php';
require_once 'php/Fakebook.php';

$fakebook = new Fakebook();

if (!isset($_GET['id']) || !isset($_GET['text'])) {
	$fakebook->error(4);
}

$id = $_GET['id'];
$text = $_GET['text'];

if ($text != "") {
	$fakebook->commentStatus($id, $text);
}

header('Location: ' . $_SERVER['HTTP_REFERER']);