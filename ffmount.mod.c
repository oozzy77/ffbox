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
	{ 0x5b8239ca, "__x86_return_thunk" },
	{ 0xbdfb6dbb, "__fentry__" },
	{ 0xb5894334, "register_filesystem" },
	{ 0x92997ed8, "_printk" },
	{ 0xda4a751, "mount_nodev" },
	{ 0x961eb8b9, "unregister_filesystem" },
	{ 0x81ddc295, "new_inode" },
	{ 0xb539a341, "current_time" },
	{ 0xb6bbfe5f, "simple_dir_inode_operations" },
	{ 0x4e6ada24, "inc_nlink" },
	{ 0xb02ddccc, "d_make_root" },
	{ 0xc88477c4, "iput" },
	{ 0x65487097, "__x86_indirect_thunk_rax" },
	{ 0xba8fbd64, "_raw_spin_lock" },
	{ 0xb5b54b34, "_raw_spin_unlock" },
	{ 0x55385e2e, "__x86_indirect_thunk_r14" },
	{ 0x92c65c79, "kill_litter_super" },
	{ 0x5c6e66e2, "simple_statfs" },
	{ 0x279a8e3f, "generic_read_dir" },
	{ 0x889f0ec6, "module_layout" },
};

MODULE_INFO(depends, "");


MODULE_INFO(srcversion, "8CBD1BD32C3F6F634F7C57B");
