#include <linux/module.h>
#define INCLUDE_VERMAGIC
#include <linux/build-salt.h>
#include <linux/elfnote-lto.h>
#include <linux/export-internal.h>
#include <linux/vermagic.h>
#include <linux/compiler.h>

BUILD_SALT;
BUILD_LTO_INFO;

MODULE_INFO(vermagic, VERMAGIC_STRING);
MODULE_INFO(name, KBUILD_MODNAME);

__visible struct module __this_module
__section(".gnu.linkonce.this_module") = {
	.name = KBUILD_MODNAME,
	.init = init_module,
#ifdef CONFIG_MODULE_UNLOAD
	.exit = cleanup_module,
#endif
	.arch = MODULE_ARCH_INIT,
};

#ifdef CONFIG_RETPOLINE
MODULE_INFO(retpoline, "Y");
#endif


static const struct modversion_info ____versions[]
__used __section("__versions") = {
	{ 0xbdfb6dbb, "__fentry__" },
	{ 0xb5894334, "register_filesystem" },
	{ 0x92997ed8, "_printk" },
	{ 0x5b8239ca, "__x86_return_thunk" },
	{ 0x65487097, "__x86_indirect_thunk_rax" },
	{ 0xda4a751, "mount_nodev" },
	{ 0x81ddc295, "new_inode" },
	{ 0x7d9b1a98, "kern_path" },
	{ 0xc88477c4, "iput" },
	{ 0x6a81c4, "path_put" },
	{ 0xb539a341, "current_time" },
	{ 0xb02ddccc, "d_make_root" },
	{ 0xd0da656b, "__stack_chk_fail" },
	{ 0x961eb8b9, "unregister_filesystem" },
	{ 0x5cdbf61e, "kill_anon_super" },
	{ 0x889f0ec6, "module_layout" },
};

MODULE_INFO(depends, "");


MODULE_INFO(srcversion, "ED7CB759FB2CC27B4BABC61");
