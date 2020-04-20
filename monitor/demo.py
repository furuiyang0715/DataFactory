# unsigned short count_CRC(unsigned char *addr, int num)
# {
# unsigned short CRC = 0xFFFF; int i;
# while (num--)
# {
# CRC ^= *addr++;
# for (i = 0; i < 8; i++)
# {
# if (CRC & 1)
# {
# CRC >>=  1; CRC ^= 0xA001;
# }
# else
# {
# CRC >>= 1;
# }
# }
# }
# return CRC;
# }


def count_CRC(addr, num):
    CRC = 0xFFFF
    while num:
        CRC ^= ''
